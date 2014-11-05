# -*- coding: utf-8 -*-
# Derrick Stuckey
# modified from code provided in class

# Import Needed Libraries
import urllib2
from bs4 import BeautifulSoup

import MySQLdb as myDB
from sqlalchemy import create_engine
import pandas as pd

#Graph stuff
import numpy as np
import matplotlib.pyplot as plt

### constants ###
#saveDir = "/Users/dstuckey/Desktop/GW/Programming for Analytics/HW 2"
saveDir = "."

### BEGIN Function Declarations ###

# Function which scrapes all course data for a given term and subject
def getCourseList(termId, subject):
    optionsUrl = 'http://my.gwu.edu/mod/pws/courses.cfm?campId=1&termId=' + termId + '&subjId=' + subject
    gwuCoursesPage = urllib2.urlopen(optionsUrl)
    soup = BeautifulSoup(gwuCoursesPage)
    
    allRows = soup.findAll('tr',{'align':'center'})
    
    tableData = []
    for row in allRows:
        eachRow = []
        cells = row.findAll('td')
        for cell in cells:
            #truncate each cell to a max size of 63 chars for DB insert
            cellStr = str(cell.text.strip())[:63]
            eachRow.append(cellStr)
        eachRow[2] = eachRow[2][:4]    # Without this there is some noise out there
        tableData.append(eachRow)
    return tableData

def getDBConnect():
    return myDB.connect(host='localhost',
                                user='root',
                                passwd='',
                                db='classwork')

# Function to save DataFrame to MySQL database
def saveToDB(df, termId, dbConnect):
    #clean up any 'NaN' fields which mysql doesn't understand
    dfClean = df.where((pd.notnull(df)), None)
    dfClean.to_sql(con=dbConnect,
                    name='sp_' + termId,
                    if_exists='replace',
                    flavor='mysql')

# Missing sqlalchemy.schema module
def readFromDB(termId, dbConnect):
    engine = create_engine('mysql+mysqldb://root:@localhost/classwork')
    
    return pd.read_sql_table('sp_' + termId, con=engine)

# Read DF from CSV
def readFromCSV(termId):
    df = pd.read_csv(saveDir + '/allCourses' + termId + '.csv')
    #clean up SUBJ column
    df.SUBJ = df.SUBJ.str.strip()
    return df

# Function to save a dataframe to CSV
def saveToCsv(df, termId):
    df.to_csv(saveDir + '/allCourses' + termId + '.csv')

# Function to scrape all subject IDs for a given term
def scrapeSubjectIds(termId):
    optionsUrl = 'http://my.gwu.edu/mod/pws/subjects.cfm?campId=1&termId=' + termId
    gwuCoursesPage = urllib2.urlopen(optionsUrl)
    soup = BeautifulSoup(gwuCoursesPage)
    
    allSubjects = soup.findAll('li')
    
    # This does he same function as the loop above
    subjectURLs = [ x.find('a')['href'] for x in allSubjects]
    
    # Parse out subject IDs from each URL
    subjIds = []
    for subjectURL in subjectURLs:
        subjKey = 'subjId='
        subjId = subjectURL[subjectURL.find(subjKey)+len(subjKey):]
        subjIds.append(str(subjId))
    
    return subjIds

def scrapeCourseData(termId):
    #Get List of Subject IDs
    subjIds = scrapeSubjectIds(termId)
    
    #Scrape course info for each subject id in the list
    allCourses = []
    headings = []
    for subj in subjIds:
    #for subj in subjIds[0:2]:
        courseList = getCourseList(termId, subj)
        headings.append(courseList.pop(0))
        allCourses = allCourses + courseList
        
    #pull out heading data (only need one instance)  
    heading = headings[0]
    #pop last field which is a blank '' string
    heading.pop()
    
    #Create dictionary dynamically from headings, allCourses lists
    courseDict = {}
    for i in range(0,len(heading)):
        courseDict[heading[i]] = [x[i] for x in allCourses]
    
    # Convert Dictionary to DataFrame
    courseDF = pd.DataFrame(courseDict)
    return courseDF

# Returns counts for each subject in the dataframe
def getCourseCounts(df):
    return df.groupby('SUBJ').COURSE.count()

# Returns a Pandas Series of the top 10 subjects by course count
def getTop10Subjects(df):
    counts = getCourseCounts(df)
    counts.sort(ascending=False)
    rawSubjNames = counts[:10].index
    topSubjects = [str(x).strip() for x in rawSubjNames]
    return topSubjects
    
# Generates a Dataframe of class counts for 2013 and 2014 for the subjects specified
def combineCourseCounts(subjects, df2013, df2014):
    #init list for each year
    countList2013 = []
    countList2014 = []
    openCountList2013 = []
    openCountList2014 = []
    
    #get full counts for each year
    counts2013 = getCourseCounts(df2013)
    counts2014 = getCourseCounts(df2014)
    openCounts2013 = getCourseCounts(df2013[df2013.STATUS == 'OPEN'])
    openCounts2014 = getCourseCounts(df2014[df2014.STATUS == 'OPEN'])
    
    for subj in subjects:
        countList2013.append(counts2013[subj])
        countList2014.append(counts2014[subj])
        openCountList2013.append(openCounts2013[subj])
        openCountList2014.append(openCounts2014[subj])
    
    return pd.DataFrame( {'subject': subjects, '2013 total': countList2013, 
                         '2014 total': countList2014, '2013 open': openCountList2013,
                         '2014 open': openCountList2014} )
    
### END Function Declarations ###



### BEGIN Program Execution ###

#Scrape all data for Spring 201_ term
termId2013 = "201301"
termId2014 = "201401"

# Scrape Course Data
courseDF2013 = scrapeCourseData(termId2013)
courseDF2014 = scrapeCourseData(termId2014)

# Save to CSV
#saveToCsv(courseDF, termId)

# Load DFs from CSV - Use this if scraping fails
#courseDF2013 = readFromCSV(termId2013)
#courseDF2014 = readFromCSV(termId2014)

# Save to DB
dbConnect = getDBConnect()
saveToDB(courseDF2013, termId2013, dbConnect)
saveToDB(courseDF2014, termId2014, dbConnect)

# Load DFs from DB
courseDF2013 = readFromDB(termId2013, dbConnect)
courseDF2014 = readFromDB(termId2014, dbConnect)

# Find the top 10 subjects by course count for 2013
topSubjs2013 = getTop10Subjects(courseDF2013)

# Generate a dataframe with counts for each year for the chosen courses
combinedCounts = combineCourseCounts(topSubjs2013, courseDF2013, courseDF2014)

### Draw a graph ###
N = len(combinedCounts) # number of data points

ind = np.arange(N)  # the x locations for the groups
width = 0.35       # the width of the bars

fig, ax = plt.subplots()

barHeight2013 = combinedCounts['2013 total']
barHeight2014 = combinedCounts['2014 total']

rects2013 = ax.bar(ind, barHeight2013, width, color='r')
rects2014 = ax.bar(ind+width, barHeight2014, width, color='y')

# add some text for labels, title and axes ticks
ax.set_ylabel('Count')
ax.set_title('Course Counts by Subject and Year')
ax.set_xticks(ind+width)
ax.set_xticklabels( combinedCounts['subject'] )

ax.legend( (rects2013[0], rects2014[0]), ('2013', '2014') )

plt.show()
### End Graph ###

### END Program Execution ###