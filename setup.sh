#!/bin/bash

echo "Setting up E-Commerce Data Pipeline environment..."

# Install Python dependencies
python -m pip install --upgrade pip
python -m pip install -r requirements.txt

echo "Dependencies installed successfully."

echo "NOTE:"
echo "Please ensure PostgreSQL is running and database is created."
