
# FROM python:latest

# WORKDIR /home/etl_service/

# COPY . /home/etl_service/

# # Update package lists
# # RUN apt-get update

# # Install OpenJDK 17 JDK
# # RUN apt-get install -y --no-install-recommends openjdk-17-jdk

# # RUN apt-get install -y redis-server
# # RUN apt install -y redis-server

# RUN pip install --no-cache-dir -r requirements.txt

# # Install cron and other dependencies
# RUN apt-get update && apt-get install -y cron

# # Copy the cron job configuration file to the container
# COPY cronjob /etc/cron.d/cronjob

# # Create the log file
# RUN touch /var/log/cron.log

# # Give execution rights to the cron job
# RUN chmod 0644 /etc/cron.d/cronjob

# # Apply the cron job
# RUN crontab /etc/cron.d/cronjob

# #Run cron and tail the log file
# CMD cron && tail -f /var/log/cron.log


FROM python:latest

WORKDIR /home/etl_service/

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . /home/etl_service/

# Copy the cron job configuration file to the container
ADD cronjob /etc/cron.d/cronjob

# Give execution rights to the cron job
RUN chmod 0644 /etc/cron.d/cronjob
# Give execution rights to the start.sh
RUN chmod 0644 /home/etl_service/start.sh

# Create the log file
RUN touch /var/log/cron.log

# Install cron and other dependencies
RUN apt-get update && apt-get install -y cron

#Run cron and tail the log file
CMD cron && tail -f /var/log/cron.log