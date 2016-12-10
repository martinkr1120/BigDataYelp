File Description:
	1.	final_project: This is a maven project containing source code written in java by using eclipse

	2.	ui: folder that contains the front end page and the server written in python

	3.	Runnerble.jar: The jar file created by final_project using maven
	
	4.	presentation.pptx: presentation slides
	
	5.	report.docx: project report

	6. 	hive table.txt: document that contains commands to create hive tables

How to comile the source code:
	1.	comile the project using command line:
		cd final_project
		mvn clean package

	2.	the runnerble jar file named final_project-0.0.1-SNAPSHOT-job.jar will be created in the target foleder.

Building the project:
		1.	hadoop latest version 2.7.2 is highly recommended
			hive 1.2.1 or latest version is required
			python 2.7 is required
			installing python package by commands: 
				pip install autobahn[asyncio]
				pip install pyhive

		2.	please download the input files from yelp website, which is https://www.yelp.com/dataset_challenge, and put all the json files into a folder in HDFS

		3.	please run the following commands one by one on the console:
			
			hadoop jar Runnerble.jar edu.neu.info7250.rerate_business.NonActiveUserRunner <input folder> <output folder>

			hadoop jar Runnerble.jar edu.neu.info7250.rerate_business.CleanNonactiveUserReviewRunner <input folder> <output folder>

			hadoop jar Runnerble.jar edu.neu.info7250.rerate_business.UserRatingHabitRunner <input folder> <output folder>

			hadoop jar Runnerble.jar edu.neu.info7250.rerate_business.UpdateUserRatingRunner <input folder> <output folder>

			hadoop jar Runnerble.jar edu.neu.info7250.rerate_business.RerateBusinessRunner <input folder> <output folder>

			hadoop jar Runnerble.jar edu.neu.info7250.rerate_business.SearchIndexForBusinessRunner <input folder> <output folder>

			hadoop jar Runnerble.jar edu.neu.info7250.rerate_business.IndexBusinessDetailRunner <input folder> <output folder>

			hadoop jar Runnerble.jar edu.neu.info7250.rerate_business.ReviewClassificationRunner <input folder> <output folder>

		4.	create hive tables using the commands in hive table.txt

Run server:
	1.	go to ui directory
	2.	run commands:
			python2.7 server2.py

Visit the website:
	go to ui directory and open the file, index.html using any web browser











