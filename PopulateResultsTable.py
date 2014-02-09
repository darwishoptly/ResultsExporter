import OptlyData
import requests
import datetime 
import time
from sqlalchemy import *
from sqlalchemy import create_engine, orm
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
import csv 
import psycopg2
import datetime
from sqlalchemy import Column, Integer, String, ForeignKey, Float, Date
from sqlalchemy import create_engine, event
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import backref, mapper, relation, sessionmaker
from pprint import pprint
import math
import requests

GAE_Auth_Cookie = "AJKiYcGfZ7qcsN70OcyyEml-bld_lJ24pWxeatugY2xhdlKK9Ih4xT5eL2DGFAgk1MjfooBb1kPKp-NjC9pstYV29c0bUw6dfJCvH-0SXqTgnuR00T7eXpkER_4q9a5-9IoMO1ix1GfK34XAC87gxKro6dLZabEUwWyXZQsaP6nHppL2hMdx4mohtsKZ3fyQha09qSudAv_m77Bbt1-XZrxwqMiFk3VxdrDgxX0Wy_rtF6gLdxDJ9uaR67dk7qg_Hlz6HwxPvYsxHyz6Wc5pYineJhYKAGL2SyFo3qP1ElIMDxKIbQBsCOSFo6uGv457eYmp88N5NIIGG_0Sh5eKsPciMbY3hPDAJPbwtiNpDssoOS4FcoVnh0PETozz2uI2CpE1nMVAYE-XGZyKnrLl647n1xUmOTnKy-lD8cAR7CddPcd7Gdlp4djGdBW1N5GrLrfiOeRl702JUs4DA07GPUwLYzc6HKEuwKgn1ugIGTDsaueOzOfcAxKumh9JIOMciF4UxyOWyFmE83bZnvqOH7oISuuTpojF414M23Xk7wF_MZJOX188veA3X2ruTkr3ejZHtoBhlGw0noZKiLSLf_Jxmi0rkgUP4QhKXcBv2f9xcsp5T2CuvXYm27Ta3kJA9W5E1gD5cQoy04yDuahNDI04SSTxyVyA2A"
start = datetime.datetime(2013, 3 ,1)

Base = declarative_base()

engine = create_engine("postgresql+psycopg2://poweruser1:h6o3W1a5R5d@success-depot.cwgfpgjh6ucc.us-west-2.rds.amazonaws.com/sd_staging")

#engine = create_engine("postgresql+psycopg2://agregory:password@localhost/postgres")
# engine = create_engine("postgresql+psycopg2://darwish:@172.20.100.16/SuccessDepotStaging")
metadata = MetaData(engine)
metadata.reflect()
Base.metadata = metadata

class UsageMonthly(Base):
	__tablename__ = "prd_usage_monthly_summary"

class Summary(Base):
	__tablename__ = "prd_account_summary"

class SummaryMonthly(Base):
	__tablename__ = "prd_account_monthly_summary"

class Results(Base):
	__tablename__ = "results"

class Account(Base):
	__tablename__ = "prd_account_master"

class User(Base):
	__tablename__ = "user_master"
	account = relationship("Account", primaryjoin=("User.account_id == foreign(Account.accountID)"), backref="user")

Session = sessionmaker(bind=engine)
s = Session()
start = datetime.datetime(2013, 3 ,1)
accounts = s.query(Account).filter(Account.plan == 'platinum', Account.startDate > start)[0:40]


def _getImpersonationEmail(account):
 	p_requests = requests.get("https://www.optimizely.com/admin/permissions/list?project_id=%s" % account.accountID, cookies={"SACSID" : GAE_Auth_Cookie})
	try:
		data = p_requests.json()['data']
	except Exception as e:
		print "AUTH ERROR", e
		raise Exception("AUTHENTICATION ERROR, ")
	for log in data:
		if log['role_name'] == "Administrator":
			return log['user_id']
	raise Exception("Impersonation Error")

def createOptlyData(account_id, project_id, email):
	try:
		D = OptlyData.client(GAE_Auth_Cookie, project_id, account_id, {"start": False, "email": email})
		D.setExperimentDescriptions(8)
		if D.exp_descriptions == {}:
			return None

		D.setVisitorCount()
		D.createTokenHash()
		D.makeExperimentsCall() ## sets variation names and original goals	
		time.sleep(2)	
		## Finish building the hash 
		# {exp_id: "goals" { goal_id :  { "name" : goal_name , variation_id: {"conversions": value, type: "type", "sum_of_squares" : SS_val_if_rev_goal, "conversion_rate" " X, "improvment" : X,  "CTB" : X }}}} 
		D.makeResultsCallSafe()	
		time.sleep(2)
		D.makeRevenueCall()
		# D.setGoals()
		D.setResultStatistics()
		return D
	except Exception as e:
		print "Error, ", e
		return None

def _getProjects(account_id, email):
	D = OptlyData.client(GAE_Auth_Cookie, account_id, account_id, {"start": False, "email": email})
	cookies = D.cookies
	D = createOptlyData(account_id, account_id, email)
	projects = [D] if D else [] 
	p_jects = requests.get("https://www.optimizely.com/api/projects.json", cookies=cookies).json()['projects']
	for p in p_jects: 
		if p['project_status'] == "Archived" or p['id'] == account_id:
			continue
		else: 
			print "CREATING DATA OBJ FOR: a_id, p_id", account_id, p['id']
			D = createOptlyData(account_id, p['id'], email)
			if D: 
				projects.append(D)
	return projects


def _filter_exp_ids(r, start):
	exp_ids = []
	for exp_id in r.exp_descriptions.keys():
		earliest = datetime.datetime.strptime(r.exp_descriptions[exp_id]['earliest'][0:-1], "%Y-%m-%dT%H:%M:%S")
		try:
			latest = datetime.datetime.strptime(r.exp_descriptions[exp_id]['latest'][0:-1], "%Y-%m-%dT%H:%M:%S")
		except: 
			latest = ""
		if earliest > start:
			exp_ids.append(exp_id)
	return exp_ids

def _increment(count_hash, key, inc , ok_to_increment=True):
	if ok_to_increment:
		count_hash[key] = count_hash[key] + inc if key in count_hash else 1

def _latest(r, exp_id):
	## Latest = "" means its running and will return an error. must decide what you will do with this. 
	try:
		latest = datetime.datetime.strptime(r.exp_descriptions[exp_id]['latest'][0:-1], "%Y-%m-%dT%H:%M:%S")
	except: 
		if r.exp_descriptions[exp_id]['status'] != "Running":
			print "ERROR: not running, but can't find end date of experiment: account_id, exp_id:", r.account_id, exp_id 
		else:
			latest = datetime.datetime.today()
	return latest

def _countExp(r, exp_id):
	global winning_experiment_count # experiments 
	global winning_goal_count
	global losing_experiment_count
	global losing_goal_count
	global pos_undecided_experiment_count
	experiment_winner_counted, experiment_loser_counted = False, False
	pos_undecided_goals, neg_undecided_goals = 0, 0
	for goal_id in r.goals[exp_id]['goals']:
		if r.goal_names[goal_id] == "Engagement":
			continue
		print "...Counting Experiment: goal", r.goal_names[goal_id]  
		goal_winner_declared, goal_loser_declared, goal_undecided = False, False, False
		winning_variations, losing_variations = 0, 0
		for var_id in r.visitor_count[exp_id]['variation'].keys():
			if var_id not in r.variation_names:
				print ".......SKIPPING variation_id:", var_id  
				continue
			print ".......variation_id:", r.variation_names[var_id]  
			if exp_id not in r.goals or var_id not in r.variation_names:
				skipped.append((exp_id, g_id, var_id))
				continue
			else: 
				imp , CTB = r.goals[exp_id]["goals"][goal_id][var_id]['improvement'], r.goals[exp_id]["goals"][goal_id][var_id]['CTB']
				print "..............imp, CTB", imp, CTB
				if imp == "-":
					continue
				elif CTB > .95:
					winning_variations += 1 
				elif CTB < .05: 
					losing_variations += 1
				elif imp > 0:
					pos_undecided_goals += 1
				elif imp < 0:
					neg_undecided_goals += 1
		print ".............. win_vars, los_vars, pos_undecided_goals, neg_undecided_goals", winning_variations, losing_variations, pos_undecided_goals, neg_undecided_goals
		latest = _latest(r, exp_id)
		if winning_variations > 0: 
			# Winner has been declared for a goal
			print "..............Adding winner"
			_increment(winning_experiment_count, (r.account_id, latest.year, latest.month), 1, not experiment_winner_counted)
			experiment_winner_counted = True
			_increment(winning_goal_count, (r.account_id, latest.year, latest.month), 1, not goal_winner_declared)
			goal_winner_declared = True
		elif losing_variations > 0:
			# Loser has been declared for a goal
			print "..............Adding loser"
			_increment(losing_experiment_count, (r.account_id, latest.year, latest.month), 1, not experiment_loser_counted)
			experiment_loser_counted = True
			_increment(losing_goal_count, (r.account_id, latest.year, latest.month), 1, not goal_loser_declared)
			goal_loser_declared = True
	# The entire Experiment has no winning or losing variations, however, there are variations with improvement > 0
	if experiment_winner_counted is False and experiment_loser_counted is False: 
		print "..............Adding goals"
		if pos_undecided_goals > 0: 
			_increment(pos_undecided_experiment_count, (r.account_id, latest.year, latest.month), 1)
		elif neg_undecided_goals > 0: 
			_increment(neg_undecided_experiment_count, (r.account_id, latest.year, latest.month), 1)

# account = accounts[0]
# 
# accounts = [account]

# Count number of goals with significance > 95%, <5%, inconclusive
winning_experiment_count = {} # experiments 
winning_goal_count = {}
losing_experiment_count = {}
losing_goal_count = {}
pos_undecided_experiment_count = {}
neg_undecided_experiment_count = {}

#  TODO add a column for Running, and enter date it was running as of. 

start_time = datetime.datetime.now()
EXP_ERRORS = []
for account in accounts[0:40]:
	try:
		email = _getImpersonationEmail(account)
	except Exception as e:
		print "ERROR: IMPERSONATION EMAIL account_id:", account.accountID, "error_type:", e
	try: 
		projects = _getProjects(account.accountID, email)
		if projects == []:
			print "ERROR: EmptyAccount account_id, email", account.accountID, email
	except Exception as e:
		print "ERROR: Exporting Results for account_id, email", account.accountID, email
	for r in projects: # r stands for results
		r.goal_names = r.setGoalNames()
		for exp_id in _filter_exp_ids(r, start):
			try:
				_countExp(r, exp_id)
			except: 
				EXP_ERRORS.append((r.account_id, r.project_id, exp_id))

end_time = datetime.datetime.now()
print "Time Taken" (end_time - start_time)


def printCounts():
	print "...........Win Experiments..........."
	pprint(winning_experiment_count)
	print "...........Lose Experiments..........."
	pprint(losing_experiment_count)
	print " ...........Win Goals..........."
	pprint(winning_goal_count)
	print "...........Lose Goals..........."
	pprint(losing_goal_count)
	print "...........Positive Undecided Experiments..........."
	pprint(pos_undecided_experiment_count)
	print "...........Negative Undecided Experiments..........."
	pprint(neg_undecided_experiment_count)	
	
# email = "alan@electricitywizard.com.au"
# account_id = 7489188
# project_id = 7489188

## TEST CASES
account = s.query(Account).filter(Account.accountID == '7489188')[0] # EWizard
account_id, project_id =  '7489188',  '20358592'
email = _getImpersonationEmail(account)
projects = _getProjects(account.accountID, email)
r = projects[0]
r.goal_names = r.setGoalNames()
# Negative Undecided Only 
exp_id = '329519695'
_countExp(r, exp_id)
print "PASS: ", winning_experiment_count == {} and winning_goal_count == {} and losing_experiment_count == {} and losing_goal_count == {} and pos_undecided_experiment_count == {} and neg_undecided_experiment_count == {(u'7489188', 2014, 2): 1}  
# Positive Undecided Only
exp_id = '377580222'
_countExp(r, exp_id)
winning_experiment_count == {} and winning_goal_count == {} and losing_experiment_count == {} and losing_goal_count == {} and pos_undecided_experiment_count == {(u'7489188', 2014, 2): 1} and neg_undecided_experiment_count == {}  
exp_id = '128827973'
_countExp(r, exp_id)
exp_id = '178451894'
_countExp(r, exp_id)

account = s.query(Account).filter(Account.accountID == '336278253').all()[0] # MSFT Sky
email = _getImpersonationEmail(account)
projects = _getProjects(account.accountID, email)
r = projects[0]
r.goal_names = r.setGoalNames()
for r in projects: 
	for exp_id in _filter_exp_ids(r, start):
		print "COUNTING EXP ID:", exp_id
		_countExp(r, exp_id)
		
account = s.query(Account).filter(Account.accountID == '223877531').all()[0] # Hubzu (highly active user)

# ERRORS
# AccountID has nothing in it 
	