#!/bin/bash
# Pipe Dream Setup Script

set -e

echo "Checking for existing environment files..."

# Root .env (only if it doesn't exist)
if [ ! -f ".env" ]; then
    echo "Creating .env..."
    cat > .env << 'EOF'
# --- Database Configuration ---
MONGO_URI=YOUR_MONGO_URI_HERE
DB_NAME=weather_db
EOF
else
    echo ".env already exists - skipping"
fi

echo
echo "Setup complete! Add MONGO_URI to .env"
