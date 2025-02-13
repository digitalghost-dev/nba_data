FROM --platform=$BUILDPLATFORM mcr.microsoft.com/dotnet/sdk:7.0 AS builder
WORKDIR /build

COPY . /build

RUN dotnet tool install retypeapp --tool-path /bin
RUN retype build --output .docker-build/

FROM httpd:latest
WORKDIR /usr/local/apache2/htdocs/

# Ensure Apache listens on port 80
RUN sed -i 's/Listen 80/Listen 8080/' /usr/local/apache2/conf/httpd.conf

COPY --from=builder /build/.docker-build/ .

EXPOSE 8080

CMD ["httpd", "-D", "FOREGROUND"]