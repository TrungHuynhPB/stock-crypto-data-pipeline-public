FROM postgres:17.4

# Install gettext-base for envsubst
RUN apt-get update && apt-get install -y --no-install-recommends gettext-base \
    && rm -rf /var/lib/apt/lists/*

# Default command
CMD ["postgres"]
