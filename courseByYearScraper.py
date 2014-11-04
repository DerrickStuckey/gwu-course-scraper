# -*- coding: utf-8 -*-
# Derrick Stuckey
# modified from code provided in class

# Import Needed Libraries
import urllib2
from bs4 import BeautifulSoup
import MySQLdb as myDB
import pandas as pd

### constants ###
saveDir = "/Users/dstuckey/Desktop/GW/Programming for Analytics/HW 2"

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
                                user='dstuckey',
                                passwd='',
                                db='classwork')

# Function to save DataFrame to MySQL database
def saveToDB(df, termId, dbConnect):
    df.to_sql(con=dbConnect,
                    name='courses_' + termId,
                    if_exists='replace',
                    flavor='mysql')

# Missing sqlalchemy.schema module
#def readFromDB(termId, dbConnect):
#    dbConnect = myDB.connect(host='localhost',
#                                user='dstuckey',
#                                passwd='',
#                                db='classwork')
#    
#    cols = ['COURSE','SUBJ']
#    return pd.read_sql_table('courses_' + termId, dbConnect, columns=cols)

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

def getCourseCounts(df):
    return df.groupby('SUBJ').COURSE.count()

def getTop10Subjects(df):
    counts = getCourseCounts(df)
    counts.sort(ascending=False)
    rawSubjNames = counts[:10].index
    topSubjects = [str(x).strip() for x in rawSubjNames]
    return topSubjects
    
def combineCourseCounts(subjects, df2013, df2014):
    #init list for each year
    countList2013 = []
    countList2014 = []
    
    #get full counts for each year
    counts2013 = getCourseCounts(df2013)
    counts2014 = getCourseCounts(df2014)
    
    for subj in subjects:
        countList2013.append(counts2013[subj])
        countList2014.append(counts2014[subj])
    
    return pd.DataFrame({'subject': subjects, '2013 count': countList2013, 
                         '2014 count': countList2014})
    
### END Function Declarations ###



### BEGIN Program Execution ###

#Scrape all data for Spring 201_ term
termId2013 = "201301"
termId2014 = "201401"

# Scrape Course Data
#courseDF = scrapeCourseData(termId)

# Save to CSV
#saveToCsv(courseDF, termId)

# Save to DB
#dbConnect = getDBConnect()
#saveToDB(courseDF, termId, dbConnect)

# Load DFs from CSV
courseDF2013 = readFromCSV(termId2013)
courseDF2014 = readFromCSV(termId2014)

# Run Queries
topSubjs2013 = getTop10Subjects(courseDF2013)

combinedCounts = combineCourseCounts(topSubjs2013, courseDF2013, courseDF2014)

### END Program Execution ###