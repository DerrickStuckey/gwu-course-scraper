# -*- coding: utf-8 -*-
# DNCS 6211: Programming for Analytics
# Raj Kanungo
#
# We start by exploring the web page - specifically inspecting elements

# What do you find?
# How should we proceed?

# I have done it my way -- I am sure someone in class will find a
# better way or one that is different from how I have approached this

# do a findAll('tr') and see what you get
# what about a finaAll('tbody') ?

# when you do a count of findAll('tr') you should get a large number
# Large ??? compared to ... ???
# Eyeball the number fo courses in ACA ... loks like these are
# fewer than 50

# So we need to look for something that is unique to these rows ...

# This code is required to get the soup in
import urllib2
from bs4 import BeautifulSoup
optionsUrl = 'http://my.gwu.edu/mod/pws/courserenumbering.cfm?subjID=ACA'
gwuCoursesPage = urllib2.urlopen(optionsUrl)
soup = BeautifulSoup(gwuCoursesPage)

# We search for all the table rows
allRows = soup.findAll('tr',{'align':'left'})

# Process all the table rows into a list of lists
tableData = []
for row in allRows:
    eachRow = []
    cells = row.findAll('td')
    for cell in cells:
    # eachRow.append(cell.getText()) # This gave everything including tags
    # eachRow.append(cell.text.strip()) # this one has unicode
        eachRow.append(str(cell.text.strip()))
    tableData.append(eachRow)
    
# Lets us try that for the subject column or the first column
subjectCol = [ x[0] for x in tableData]
# and we get
# IndexError: list index out of range
# >>> Traceback (most recent call last):
# File "<stdin>", line 1, in <module>

# Whoa!
# # Please investigate ... click into tableData in the variable explorer
# Go all the way down in the Variable explorer
# Note that there are two lists that are troublesome

# remove the troublesome list elements
tableData = [ x for x in tableData if x != [] ]
tableData = [ x for x in tableData if x[0] != '' ]

# Now run the following command for obtaining the subject 'column'
subjectCol = [ x[0] for x in tableData]

# Looks like all is well now

# We can now run all the other commands for the other columns
descriptionCol = [ x[1] for x in tableData]
oldnumberCol = [ x[2] for x in tableData]
newnumberCol = [ x[3] for x in tableData]
coursetitleCol = [ x[4] for x in tableData]

# Now create the dictionary that needs to be provided as input for the data frame
mydict = {
            'subject' : subjectCol,
            'description' : descriptionCol,
            'oldnumber' : oldnumberCol,
            'newnumber' : newnumberCol,
            'coursetitle' : coursetitleCol
        }
        
# You can access the values of the dictionary by the key
mydict['subject']
mydict['newnumber']

# Now we are ready to make a dataframe from this dictionary
import pandas as pd
myDF = pd.DataFrame(mydict)

# Note that the column names are arranged in ascending order

# Now this can be saved as a csv file
myDF.to_csv('/tmp/mysubjectsasCSV.csv')

# MySQL stuff...
import MySQLdb as myDB

dbConnect = myDB.connect(host='localhost',
                            user='dstuckey',
                            passwd='',
                            db='classwork') # I am assuming that this database exists

myDF.to_sql(con=dbConnect,
                name='tableOfCourses',
                if_exists='replace',
                flavor='mysql')

# Find all the option tags and get a list of all the values
subjects = soup.findAll('option')
courseList = []
for asubject in subjects:
    courseList.append(asubject['value'])
    
# remove the first element - which is blank
courseList.pop(0) 

# OK now to automate
# Create a list of URLs that we will cycle through
baseURL = 'http://my.gwu.edu/mod/pws/courserenumbering.cfm?subjID='
myURLs = [ baseURL + aSpecificSubject for aSpecificSubject in courseList ]

# define a function that will scrape a given URL
def getCourseList(aURL):
    optionsUrl = aURL
    gwuCoursesPage = urllib2.urlopen(optionsUrl)
    soup = BeautifulSoup(gwuCoursesPage)
    
    allRows = soup.findAll('tr',{'align':'left'})
    
    tableData = []
    for row in allRows:
        eachRow = []
        cells = row.findAll('td')
        for cell in cells:
            eachRow.append(str(cell.text.strip()))
        tableData.append(eachRow) 
    tableData = [ x for x in tableData if x != [] ]
    tableData = [ x for x in tableData if x[0] != '' ]    
    return tableData

# Now to scrape all courses

#################### uncomment for submission ####################
#allCourses = []
#for aURL in myURLs:
#    courseList = getCourseList(aURL)
#    allCourses = allCourses + courseList

###### my code: #######

#convert course list to data frame
#allCoursesDF = pd.DataFrame(allCourses)

#save as .csv just in case
#allCoursesDF.to_csv('/tmp/allCourses.csv')

#save all courses to DB
#allCoursesDF.to_sql(con=dbConnect, name='allCourses', if_exists='replace',flavor='mysql')
#################### uncomment for submission ####################


