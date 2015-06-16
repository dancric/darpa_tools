import numpy as np
import csv
import sqlite3
from sqlalchemy import *
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import itertools
import functools
import pandas as pd
import matplotlib.pyplot as plt

Base = declarative_base()

#Database Tables
class Budget(Base):
	__tablename__ = 'budget'

	id = Column(Integer, primary_key=True)
	appropriation = Column(Text)
	component = Column(Text)
	appropriation_name = Column(Text)
	organization = Column(Text)
	org_name = Column(Text)
	line_number = Column(Integer)
	budget_account = Column(Text)
	budget_name = Column(Text)
	program_element_id = Column(Text)
	program_element_name = Column(Text)
	fiscal_year = Column(Integer)
	amount_year = Column(Integer)
	amount = Column(Integer)

class Consolidated(Base):
	__tablename__ = 'consolidated'
	
	id = Column(Integer, primary_key=True)
	appropriation = Column(Text)
	component = Column(Text)
	appropriation_name = Column(Text)
	organization = Column(Text)
	org_name = Column(Text)
	line_number = Column(Integer)
	budget_account = Column(Text)
	budget_name = Column(Text)
	program_element_id = Column(Text, index=True)
	program_element_name = Column(Text)
	year_1996 = Column(Integer)
	year_1997 = Column(Integer)
	year_1998 = Column(Integer)
	year_1999 = Column(Integer)
	year_2000 = Column(Integer)
	year_2001 = Column(Integer)
	year_2002 = Column(Integer)
	year_2003 = Column(Integer)
	year_2004 = Column(Integer)
	year_2005 = Column(Integer)
	year_2006 = Column(Integer)
	year_2007 = Column(Integer)
	year_2008 = Column(Integer)
	year_2009 = Column(Integer)
	year_2010 = Column(Integer)
	year_2011 = Column(Integer)
	year_2012 = Column(Integer)
	year_2013 = Column(Integer)
	year_2014 = Column(Integer)
	year_2015 = Column(Integer)

#Setup database
engine = create_engine('sqlite:///data.db', module=sqlite3)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)

#Convert data into CSV files

def read_data(year):
	with open('%s.txt' % year, newline='') as csvfile:
		spamreader = csv.reader(csvfile, delimiter='\t', quotechar='"')
		for row in spamreader:
			print(', '.join(row))

def read_header(year):
	with open('%s.txt' % year, newline='') as csvfile:
		spamreader = csv.reader(csvfile, delimiter='\t', quotechar='"')
		for row in spamreader:
			return row

def extract_header():
	header = []

	for year in range(1998,2016):
		if year == 2000:
			continue
		header.append(read_header(str(year)))

	header = [elem for iterable in header for elem in iterable]

	#header.sort()
	return header

#Import CSV to Sqlite database

def create_budget_table():
	conn = sqlite3.connect('data.db')
	c = conn.cursor()
	c.execute('''CREATE TABLE budget
             (appropriation text, component text, appropriation_name text, organization text, org_name text,
             	line_number integer, budget_account text, budget_name text, program_element_id text, program_element_name text, 
             	fiscal_year integer, amount_year integer, amount integer)''')
	conn.close()

def import_data():
	conn = sqlite3.connect('data.db')
	c = conn.cursor()

	for year in range(1998,2016):
		if year == 2000:
			continue
		with open('%s.txt' % year, newline='') as csvfile:
			reader = csv.DictReader(csvfile, delimiter='\t', quotechar='"')
			for row in reader:
				process_row(row, year, c)
				conn.commit()

	c.close()

def process_row(row, fiscal_year, c):
	#Determine years of budget data
	years = []
	for item in row.keys():
		if item.isnumeric():
			years.append(item)

	for year in years:
		c.execute("INSERT INTO budget VALUES (\'" + row.get('appropriation',"").strip() + "\',\'" + row.get('component',"").strip() + "\',\'" + row.get('appropriation_name',"").strip() + "\',\'" + row.get('organization',"").strip() + "\',\'" + row.get('org_name',"").strip() + "\',\'" + row.get('line_number',"").strip() + "\',\'" + row.get('budget_account',"").strip() + "\',\'" + row.get('budget_name',"").strip() + "\',\'" + row.get('program_element_id',"").strip() + "\',\'" + row.get('program_element_name',"").strip() + "\',\'" + str(fiscal_year) + "\',\'" + str(year) + "\',\'" + row.get(year,"").strip() + "\')")

#Consolidate database information

def build_consolidated_budget():
	session = Session()

	element_ids = list(session.query(distinct(Budget.program_element_id))) #get all unique program_element_ids
	element_ids = [element[0] for element in element_ids]
	create_empty_consolidated_rows(element_ids, session)
	consolidate_data(element_ids,session)
	properly_inflate()

def create_empty_consolidated_rows(element_ids, session):
	for element_id in element_ids:
		new_entry = Consolidated(program_element_id=element_id)
		session.add(new_entry)

		#Setup default 0 for each budget number
		for year in range(1996,2016):
			setattr(new_entry,"year_%s" % year, 0)
		
	session.commit()

def consolidate_data(element_ids, session):
	for element_id in element_ids:
		#Handle Classified programs separately
		if element_id == "9999999999" or element_id == "XXXXXXXXXXX" or element_id == "9999999":
			continue
		years = session.query(distinct(Budget.amount_year)).filter(Budget.program_element_id == element_id).all()
		years = [year[0] for year in years]

		consolidated_entry = session.query(Consolidated).filter(Consolidated.program_element_id == element_id).one()

		for year in years:
			amount = session.query(Budget.amount).filter(and_(Budget.program_element_id == element_id, Budget.amount_year == year)).order_by(Budget.fiscal_year.desc()).first()

			amount = str(amount[0]).replace(",","") #remove any excess commas due to the data file
			if amount == "":
				amount = 0
			else:
				amount = int(amount)

			setattr(consolidated_entry,"year_%s" % year, amount)

		session.commit()

def properly_inflate(session):
	raw_inflation = [1.58,1.59,2.93,1.63,2.63,0.03,4.28,2.08,3.99,2.97,1.93,2.60,1.14,3.73,2.74,1.67,1.57,3.04] #2014 to 1997
	raw_inflation.reverse()
	raw_inflation = [percentage/100 + 1 for percentage in raw_inflation]
	inflation_2015 = [] #the single multiple from one year all the way to 2015

	#Multiply all the years together - by year - so that we just multiple once later
	for i in range(0,len(raw_inflation)):
		inflation_2015.append(functools.reduce(lambda x, y: x*y, raw_inflation[i:]))

	base_year = 1996

	consolidated = session.query(Consolidated).all()

	print("Inflating")
	for entry in consolidated:
		for i in range(0,18):
			if getattr(entry,"year_%s" % (base_year+i)) == '':
				setattr(entry, "year_%s" % (base_year+i), 0)
			else:
				setattr(entry, "year_%s" % (base_year+i), getattr(entry,"year_%s" % (base_year+i)) * inflation_2015[i])

	print("Committing")
	session.commit()

def final_cleanup(session):
	#Multiply by thousands and clean off floats
	consolidated = session.query(Consolidated).all()

	for entry in consolidated:
		for year in range(1996,2016):
			setattr(entry, "year_%s" % year, int(getattr(entry,"year_%s" % year) * 1000))

	session.commit()

def get_matrix(session):
	dataframe = pd.read_sql(sql="SELECT program_element_id,year_1996,year_1997,year_1998,year_1999,year_2000,year_2001,year_2002,year_2003,year_2004,year_2005,year_2006,year_2007,year_2008,year_2009,year_2010,year_2011,year_2012,year_2013,year_2014,year_2015 FROM consolidated", con=engine)
	data = dataframe.set_index('program_element_id')
	return data

def add_appropriation_column(element_ids, session):
	for element_id in element_ids:
		component = session.query(Budget.component).filter(and_(Budget.program_element_id == element_id,Budget.component != "")).first()
		print(component)


def analysis(data,session):
	#Aggregate Budget by Year with Presidential Terms Marked
	years = list(range(1996,2016))
	budgets = np.sum(data)

	plt.plot(years,budgets,'bo',years,budgets,'b',linewidth=2.0)
	plt.xlabel("Fiscal Year")
	plt.ylabel("Budget (in Tens of Billions)")
	plt.ylim(0,80000000000)
	plt.axvline(x=1997,linewidth=1, color='#cccccc')
	plt.axvline(x=2001,linewidth=1, color='#cccccc')
	plt.axvline(x=2005,linewidth=1, color='#cccccc')
	plt.axvline(x=2009,linewidth=1, color='#cccccc')
	plt.axvline(x=2013,linewidth=1, color='#cccccc')
	plt.savefig('Graph1.pdf', format="pdf")
	plt.clf()

	#Number of active line items by year

	line_items = np.sum(data != 0)

	plt.plot(years,line_items,'bo',years,line_items,'b',linewidth=2.0)
	plt.xlabel("Fiscal Year")
	plt.ylabel("Number of Line Items")
	plt.ylim(0,1000)
	plt.savefig('Graph2.pdf', format="pdf")
	plt.clf()

	#largest line items
	expenditures = np.sum(data, axis=1)
	expenditures.sort()
	expenditures[0:10]
	top = list(expenditures[-11:-1].index)

	#smallest line items
	expenditures.sort(ascending=False)
	low_indices = set(list(range(0,len(expenditures)))*(expenditures > 0).values)
	indices = list(low_indices)[1:11] #the lowest index will be 0 due to the False valuce above, so we need to cut that off
	expenditures[indices]
	bottom = list(expenditures[indices].index)

	#categories chart
	data = categories_total(session)

	lines = plt.plot(data[0:7].T,linewidth=2)
	plt.gcf().subplots_adjust(bottom=0.15,left=0.1)
	plt.xticks(range(0,20), range(1996,2016), rotation='vertical')
	plt.legend(lines, ('1','2','3','4','5','6','7'),loc=2)
	plt.xlabel("Fiscal Year")
	plt.ylabel("Budget (Tens of Billions of Dollars)")
	plt.savefig('Graph3.pdf', format="pdf")
	plt.clf()

	lines = plt.plot(data[0:1].T,linewidth=2)
	plt.gcf().subplots_adjust(bottom=0.15,left=0.1)
	plt.xticks(range(0,20), range(1996,2016), rotation='vertical')
	plt.xlabel("Fiscal Year")
	plt.ylabel("Budget (Tens of Billions of Dollars)")
	plt.savefig('Graph4.pdf', format="pdf")
	plt.clf()

	#Development Series (replaced by missile shield graph)
	values = get_development_series(session)
	lines = plt.plot(values.T,linewidth=2)
	plt.legend(lines, ('Missile Shield','F-22','JSF'),loc=2)
	plt.xticks(range(0,20), range(1996,2016), rotation='vertical')
	plt.xlabel("Fiscal Year")
	plt.ylabel("Budget (Billions of Dollars)")
	plt.savefig('Graph5.pdf', format="pdf")
	plt.clf()

	#Missile shield expenditures
	values = get_missile_shield(session)
	lines = plt.plot(values.T,linewidth=2)
	plt.xticks(range(0,20), range(1996,2016), rotation='vertical')
	plt.xlabel("Fiscal Year")
	plt.ylabel("Budget (Tens of Billions of Dollars)")
	plt.ylim(0,11000000000)
	plt.savefig('Graph5.pdf', format="pdf")
	plt.clf()

	expenditures = get_component_series(session)
	lines = plt.plot(expenditures.T,linewidth=2)
	plt.gcf().subplots_adjust(bottom=0.15,left=0.1)
	plt.xticks(range(0,20), range(1996,2016), rotation='vertical')
	plt.legend(lines, ('A','D','N','F'),loc=2)
	plt.xlabel("Fiscal Year")
	plt.ylabel("Budget (Tens of Billions of Dollars)")
	plt.ylim(0,25000000000)
	plt.savefig('Graph6.pdf', format="pdf")
	plt.clf()

	#Relative budgets of the departments by year
	relative_budgets = expenditures / np.sum(expenditures)
	lines = plt.plot(relative_budgets.T,linewidth=2)
	plt.gcf().subplots_adjust(bottom=0.15,left=0.1)
	plt.xticks(range(0,20), range(1996,2016), rotation='vertical')
	plt.legend(lines, ('A','D','N','F'),loc=2)
	plt.xlabel("Fiscal Year")
	plt.ylabel("Proportion of DARPA's Budget")
	plt.ylim(0,1)
	plt.savefig('Graph7.pdf', format="pdf")
	plt.clf()

	expenditures = get_component_series(session) # have to change the SQL to only graph budget_account = 1!
	lines = plt.plot(expenditures.T,linewidth=2)
	plt.gcf().subplots_adjust(bottom=0.15,left=0.1)
	plt.xticks(range(0,20), range(1996,2016), rotation='vertical')
	plt.legend(lines, ('A','D','N','F'),loc=1)
	plt.xlabel("Fiscal Year")
	plt.ylabel("Budget (Billions of Dollars)")
	plt.ylim(0,1000000000)
	plt.savefig('Graph8.pdf', format="pdf")
	plt.clf()


def get_line_item_names(element_ids,session):
	for element in element_ids:
		rows = session.query(distinct(Budget.program_element_name)).filter(Budget.program_element_id == element).all()
		for row in rows:
			print("%s: %s" % (element,row[0].title()))

def budget_categories(element_ids,session):
	for element in element_ids:
		budget_accounts = session.query(distinct(Budget.budget_account)).filter(Budget.program_element_id==element).all()
		budget_nums = set()
		for row in budget_accounts:
			if row[0] != "":
				budget_nums.add(int(row[0]))
		if len(budget_nums) == 1:
			consolidated = session.query(Consolidated).filter(Consolidated.program_element_id == element).one()
			consolidated.budget_account = budget_nums.pop()
			session.commit()
		else:
			print("Bad: %s" % element)

def fix_categories(session):
	rows = session.query(Budget).all()

	for row in rows:
		row.budget_account = int(str(row.budget_account))

	session.commit()

def cleanup_amounts(session):
	rows = session.query(Budget).all()

	for row in rows:
		string = str(row.amount).replace(",","")
		if string == '':
			string = "0"
		row.amount = int(string)

	session.commit()

def categories_total(session):
	index = list(range(1,8)) #budget categories 1-7
	index.append(99) # classified category
	columns = list(range(1996,2016))
	data = pd.DataFrame(index=index,columns=columns)
	data.fillna(0)

	aggregate = None

	#Translate between budget year and fiscal year
	year_values = list(zip(range(1996,2016),range(1996,2016)))
	year_values[0] = (1998,1996)
	year_values[1] = (1998,1997)
	year_values[4] = (2001,2000)

	for i in data.index:
		for year in range(0,20):
			dataframe = pd.read_sql(sql="SELECT amount,id FROM budget WHERE budget_account = %s AND fiscal_year==%s AND amount_year == %s" % (i,year_values[year][0],year_values[year][1]), con=engine)
			values = dataframe.set_index('id')

			aggregate = np.sum(values)[0]
			data.set_value(i,year+1996,aggregate)

	return data * 1000 #correct since all original data is in thousands

def basic_science_changes(session):
	dataframe = pd.read_sql(sql="SELECT program_element_id, %s FROM consolidated WHERE budget_account = 1" % all_years_select(), con=engine)
	values = dataframe.set_index('program_element_id')

	expenditures = np.sum(values, axis=1)
	expenditures.sort(ascending=False)

	variances = np.var(values, axis=1)
	variances.sort()

	basic_science_items = session.query(Consolidated).filter(Consolidated.budget_account == 1).all()

	defense_sciences = np.sum(expenditures[['0601153N', '0601102F','0601102A','0601101E']])

def product_development_changes(session):
	dataframe = pd.read_sql(sql="SELECT program_element_id, %s FROM consolidated WHERE budget_account = 4 or budget_account = 5" % all_years_select(), con=engine)
	values = dataframe.set_index('program_element_id')

	expenditures = np.sum(values, axis=1)
	expenditures.sort(ascending=False)

	variances = np.var(values, axis=1)
	variances.sort(ascending=False)


def all_years_select():
	fields = ["year_%s" % year for year in range(1996,2016)]
	return ','.join(fields)

def university_basic_science(session):
	university_line_items = '"0601104A","0601103D","0601111D","0601103D8Z","0601111D8Z","0601103A","0601103N","0601103F","0601000BR"'
	dataframe = pd.read_sql(sql="SELECT program_element_id, %s FROM consolidated WHERE budget_account = 1 AND program_element_id in (%s)" % (all_years_select(),university_line_items), con=engine)
	values = dataframe.set_index('program_element_id')

	np.sum(values)

def get_development_series(session):
	dataframe = pd.read_sql(sql="SELECT program_element_id, %s FROM consolidated WHERE program_element_id in (%s)" % (all_years_select(),"'0603882C','0604239F','0604800N'"), con=engine)
	values = dataframe.set_index('program_element_id')
	return values

#Check whether each line item is ever budgeted under different components
def update_component(element_ids,session):
	for element in element_ids:
		rows = session.query(distinct(Budget.component)).filter(and_(Budget.program_element_id == element, Budget.component != "")).all()
		if len(rows) == 1:
			consolidated = session.query(Consolidated).filter(Consolidated.program_element_id == element).one()
			consolidated.component = rows[0][0]
			session.commit()

	blanks = session.query(Consolidated.program_element_id).filter(Consolidated.component == None).all()
	blanks = [row[0] for row in blanks]

	for element in blanks:
		rows = session.query(distinct(Budget.appropriation)).filter(Budget.program_element_id == element).all()
		if len(rows) == 1:
			consolidated = session.query(Consolidated).filter(Consolidated.program_element_id == element).one()
			consolidated.component = rows[0][0][4] # the fifth character of every appropriation is the component code
			session.commit()

#Gets the budget by department
#We can change the SQL to add "AND budget_account = 1" to get just basic science research funding
def get_component_series(session):
	keys = ['D','A','N','F']

	components = dict()

	for key in keys:
		dataframe = pd.read_sql(sql="SELECT program_element_id, %s FROM consolidated WHERE component = '%s' AND budget_account = 1" % (all_years_select(),key), con=engine)
		components[key] = dataframe.set_index('program_element_id')

	aggregates = {key: np.sum(components[key]) for key in keys}
	expenditures = pd.DataFrame(aggregates).T

	return expenditures

def get_missile_shield(session):
	rows = session.query(Budget.program_element_name,Budget.program_element_id).all()

	ids = set()
	for row in rows:
		if "missile defense" in (row[0].lower()):
			ids.add(row[1])

	missile_ids = "','".join(list(ids))

	dataframe = pd.read_sql(sql="SELECT program_element_id, %s FROM consolidated WHERE program_element_id in ('%s')" % (all_years_select(),missile_ids), con=engine)
	values = dataframe.set_index('program_element_id')
	return np.sum(values)

def get_university(session):
	rows = session.query(Budget.program_element_name,Budget.program_element_id).all()

	ids = set()
	for row in rows:
		if "university" in (row[0].lower()):
			ids.add(row[1])

	university_ids = "','".join(list(ids))

	dataframe = pd.read_sql(sql="SELECT program_element_id, %s FROM consolidated WHERE program_element_id in ('%s')" % (all_years_select(),university_ids), con=engine)
	values = dataframe.set_index('program_element_id')
	return np.sum(values)


