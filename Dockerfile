# First stage: Builder
FROM python:3.12-slim-bookworm AS builder

# Install system dependencies needed for building Python packages
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    software-properties-common \
    git \
    ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# Download and install `uv`
ADD https://astral.sh/uv/install.sh /uv-installer.sh
RUN sh /uv-installer.sh && rm /uv-installer.sh

# Ensure `uv` binary is on the PATH
ENV PATH="/root/.local/bin/:$PATH"

# Set work directory
WORKDIR /app

# Copy dependency files first for better caching
COPY pyproject.toml ./
COPY uv.lock ./

# Install dependencies using uv
RUN uv venv .venv && uv sync

# Second stage: Minimal runtime environment
FROM python:3.12-slim-bookworm AS runtime

# Create a non-root user
RUN groupadd -r streamlit_group && useradd -r -g streamlit_group streamlit_user

# Set work directory
WORKDIR /app

# Copy necessary files from the builder stage
COPY --from=builder /app/.venv /app/.venv
COPY --from=builder /app /app
COPY streamlit_components streamlit_components
COPY streamlit_app.py .

# Ensure `uv` is in the path
ENV PATH="/root/.local/bin:$PATH"

ENV HOME=/app

# Set environment variables for virtual environment
ENV VIRTUAL_ENV=/app/.venv
ENV PATH="$VIRTUAL_ENV/bin:$PATH"

# Ensure correct permissions
RUN chown -R streamlit_user:streamlit_group /app

# Expose Streamlit port
EXPOSE 8501

# Healthcheck
HEALTHCHECK CMD curl --fail http://localhost:8501/_stcore/health || exit 1

# Switch to non-root user
USER streamlit_user

# Run Streamlit
ENTRYPOINT ["streamlit", "run", "streamlit_app.py", "--server.port=8501", "--server.address=0.0.0.0", "--theme.primaryColor=indigo", "--theme.textColor=black", "--theme.backgroundColor=#FFF", "--theme.secondaryBackgroundColor=#FFF"]