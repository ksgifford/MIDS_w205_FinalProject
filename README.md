# MIDS_w205_FinalProject
Final project code for MIDS W205 - Created by Kevin Gifford, Mike Gruzynski, and Jay Zuniga
STEP BY STEP:
1. Log into AWS and Launch New Instance
    - Under Community AMI, search for UCB W205 SPRING 2016
    - A m3.large class instance is recommended to ensure adequate network performance during data downloads.
    - Configure the Instance
      - Recommended: Add protection against accidental termination
      - Add Storage and increase root volume size from the default of 30 GB to at least 100 GB
    - Add Tags as you desire
    - Configure Security Group to allow access from the following port ranges:
      - 4040, 5432, 7180, 8080, 8088, 50070, 10000 ALL WITH 0.0.0.0/0, ::/0 as last colunmn
    - Review and Launch
    - Creation of a new project-specific key pair is recommended. Be sure to download and store it in a secure location.
    - Ensure the instance is running.

2. Connect to AWS EC2 instance via ssh

3. Identify the Root Device and Mount to /data
    - Use fdisk -l to identify the 100 GB root volume
    - Mount this device to /data

4. Grant Permissions to Run Setup Scripts
    - chmod a+rwx /data
	
5. Download setup script, make it executable, and run.
    - wget https://s3.amazonaws.com/ucbdatasciencew205/setup_ucb_complete_plus_postgres.sh
    - chmod +x ./setup_ucb_complete_plus_postgres.sh
    - ./setup_ucb_complete_plus_postgres.sh <FROM ABOVE THE DATA VOLUME>
      - EXAMPLE: ./setup_ucb_complete_plus_postgres.sh /dev/xvda1
	- cd /data
	- ./start_postgres.sh
	
6. Create Postgres database and dataframes
    - Launch postgres command line interface:
      - psql --username=postgres
    - From the postgres CLI:
      - CREATE DATABASE final_project;
	- Quit postgres
	
7. Clone Github Repository and Build Projectrun git clone directory and run bash.sh script
    - From /data, run the following:
      - git clone https://github.com/ksgifford/MIDS_w205_FinalProject.git
	    - chmod +x /data/MIDS_w205_FinalProject/*.sh
	    - /data/MIDS_w205_FinalProject/bash.sh
	  - *** NOTE: YOU WILL HAVE TO DO SOME INTERACTIVE COMMAND LINE ENTERS FOR ANACONDA INSTALL
	    - Just hit enter when prompted and enter 'y' for all y/n questions
	    
8. When the script is complete, the data will be available for queries in the Postgres database.
    - The folder /data/MIDS_w205_FinalProject/viz contains a file named viz_queries.sql that contains a series of SQL queries used for building our visualizations. Visualization was conducted using the Plotly database connector app to connect to the postgres server on our running AWS instance. Instructions for installation of the database connector for your operating system can be found here: https://plot.ly/database-connectors/ 

