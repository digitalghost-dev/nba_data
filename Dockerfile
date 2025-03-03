# First stage: Builder
FROM python:3.12-slim-bookworm AS builder

# Install system dependencies needed for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    software-properties-common \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install uv package manager
RUN apt-get update && apt-get install -y --no-install-recommends curl ca-certificates

# Download the latest installer
ADD https://astral.sh/uv/install.sh /uv-installer.sh

# Run the installer then remove it
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure the installed binary is on the `PATH`
ENV PATH="/root/.local/bin/:$PATH"

# Set work directory
WORKDIR /app

# Copy dependency file first for better caching
COPY pyproject.toml ./
COPY uv.lock ./

RUN which uv && uv --version

# Install dependencies using uv
RUN uv sync

# Second stage: Minimal runtime environment
FROM python:3.12-slim-bookworm AS runtime

# Create a non-root user
RUN groupadd -r streamlit_group && useradd -r -g streamlit_group streamlit_user

# Set work directory
WORKDIR /app

# Copy only necessary files from the builder stage
COPY --from=builder /root/.local/bin/uv /root/.local/bin/uv
COPY --from=builder /usr/local /usr/local
COPY streamlit_components streamlit_components
COPY streamlit_app.py .

# Ensure `uv` is in the path
ENV PATH="/root/.local/bin:$PATH"

# Create and set permissions for `.cache/uv`
RUN mkdir -p /home/streamlit_user/.cache/uv && \
    chown -R streamlit_user:streamlit_group /home/streamlit_user/.cache

# Fix permissions for the `streamlit` binary and scripts
RUN chmod -R +x /root/.local/bin/uv && \
    chown -R streamlit_user:streamlit_group /root/.local/bin

# Set correct permissions
RUN chown -R streamlit_user:streamlit_group /app

# Expose Streamlit port
EXPOSE 8501

# Healthcheck
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Switch to non-root user
USER streamlit_user

# Run Streamlit
ENTRYPOINT ["uv", "run", "streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0", "--theme.primaryColor=indigo", "--theme.textColor=black", "--theme.backgroundColor=#FFF", "--theme.secondaryBackgroundColor=#FFF"]