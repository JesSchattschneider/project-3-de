# Use Python 3.10 slim version as the base image
FROM python:3.9

# Copy everything in your current directory to the /app directory in the container
COPY . /app

# Set the working directory inside the container to /app
WORKDIR /app

# Install Python dependencies listed in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Expose port 80 (default Streamlit port)
EXPOSE 80

# Command to run the Streamlit app
CMD ["streamlit", "run", "app.py", "--server.port=80", "--server.address=0.0.0.0"]
