# MIDS_w205_FinalProject
Final project code for MIDS W205 - Created by Kevin Gifford, Mike Gruzynski, and Jay Zuniga
STEP BY STEP:
1. Log into AWS and Launch New Instance
    - Go To Community AMI and search for UCB W205 SPRING 2016
    - choose m3.large
    - Hit Next Configure: Instance Details
    - add: Protect against accidental termination
    - Next: add Storage
    - change /dev/sda1 from 30 to 100 gb
    - Hit Next Add Tags
    - Add another tag
    - key: w205_final_project_key value: w205_final_project_value
    - Next Configure Security Group
    - Port Range: 4040, 7180, 8080, 8088, 50070, 10000 ALL WITH 0.0.0.0/0, ::/0 as last colunmn
    - Hit next review
    - Hit Launch
    - Then change top to Create a new key pair: name it w205_final and HIT DOWNLOAD KEY PAIR

2. Connect to AWS EC2 instance

3. open up /data
    - chmod a+rwx /data

4. Figure out name of data volume
    - fdisk -l
	
5. get setup script and make it executable and run
    - wget https://s3.amazonaws.com/ucbdatasciencew205/setup_ucb_complete_plus_postgres.sh
    - chmod +x ./setup_ucb_complete_plus_postgres.sh
    - ./setup_ucb_complete_plus_postgres.sh <FROM ABOVE THE DATA VOLUME>
    - mine command : ./setup_ucb_complete_plus_postgres.sh /dev/xvda1
    - hit enter to continue
	- cd /data
	- ./start_postgres.sh
	
6. Create postgres database and dataframes
    - input below in AWS command prompt:
    - psql --username=postgres
    - CREATE DATABASE final_project;
	- Hit ctrl + d to exit
	
7. run git clone directory and run bash.sh script
    - git clone https://github.com/ksgifford/MIDS_w205_FinalProject.git
	- chmod +x /data/MIDS_w205_FinalProject/*.sh
	- /data/MIDS_w205_FinalProject/bash.sh
	- ** NOTE YOU WILL HAVE TO DO SOME INTERACTIVE COMMAND LINE ENTERS FOR ANACONDA INSTALL
	    - Just hit enter when prompted and enter 'y' for all y/n questions


