import OptlyData
from dateutil.relativedelta import relativedelta
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

GAE_Auth_Cookie = "AJKiYcHuSkW2TPm2Rp-NPJzg-aPh41QGh2udCsyqrNxM14tKCt5Z_NQh-vmoBkKhOr248LSrKoWBHu-ZF3fikhnW1Q0H4LMKK5cJJEvIzlVW-cV9MnvP7UPlSASk46234T0YLw0rlRZxNi227TKljzkih_1b2QdG0bdvp4qcDANQrnRoCSnyfAk1OH1MYKRTSCJQdvDWI_PT5dN3leJdnN0GRkeTSVANAHm-PB4ycVGNl8U20fEnVIUDfgBwi2tjV7YiHS6TgsqQxL361jMXrpQ7qFoNTrhEwtFiGR8x9cPK19cwOCmnac3jG9HcBw17cBTj7Zr7lCZ3D1cgpYVpmk-gm5eLSDB2xJ5uve6tPkULSnziHUZR5L6BGxVW6ZOIxXDQv4HqHjfWhNpRcugCgAcPrbqQ4zHkAPZV8kbPTSnVZ29GcjCxkpd2KtltQ9C66Mk6F8zp937QJ2-d6I_0bgLiUEpsam65_UbmBsxT-SYp2vEPdKQJzEsb5eQIQ4WyTz5mJGBQgrr05y5x3c5i6xWJdqklClTsZ0fanC5Lyznw1Tsiwi_Q1U5VU0y1Uyu4z3CRG0ObmzqMny8wiYysiw2NYtyP1RdMHyQuYlEDXX_tN2sRhymtYYy2tolTspY--LmR4hPC8Vj6y7Fm1WP41M5LwKPLx8a2BQ"

Base = declarative_base()
ERRORS = []
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
	__tablename__ = "d_results"

class Account(Base):
	__tablename__ = "prd_account_master"

class User(Base):
	__tablename__ = "user_master"
	account = relationship("Account", primaryjoin=("User.account_id == foreign(Account.accountID)"), backref="user")

class AppUsage(Base):
	__tablename__ = "d_app_usage"

Session = sessionmaker(bind=engine)
s = Session()
start = datetime.datetime(2013, 1 ,1)
accounts = s.query(Summary).filter(Summary.plan == 'platinum', Summary.start_date >= start).all()


def _getImpersonationEmail(account):
 	p_requests = requests.get("https://www.optimizely.com/admin/permissions/list?project_id=%s" % account.account_id, cookies={"SACSID" : GAE_Auth_Cookie})
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
	print "CREATING FOR, ", account_id, project_id, email
	try:
		D = OptlyData.client(GAE_Auth_Cookie, project_id, account_id, {"start": False, "email": email})
		# TODO MAKE THIS 13 DYNAMIC
		D.setExperimentDescriptions(13)
		if D.exp_descriptions == {}:
			return None
		
		D.setVisitorCount()
		D.createTokenHash()
		D.makeExperimentsCall() ## sets variation names and original goals	
		## Finish building the hash 
		# {exp_id: "goals" { goal_id :  { "name" : goal_name , variation_id: {"conversions": value, type: "type", "sum_of_squares" : SS_val_if_rev_goal, "conversion_rate" " X, "improvment" : X,  "CTB" : X }}}} 
		try:
			D.makeResultsCallSlow(10)	
		except: 
			D.makeResultsCallSlow(5)	
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
		if p['project_status'] == "Archived" or str(p['id']) == account_id:
			continue
		else: 
			print "CREATING DATA OBJ FOR: a_id, p_id", account_id, p['id']
			try:
				D = createOptlyData(account_id, p['id'], email)
			except Exception as e: 
				ERRORS.append((account_id, project_id, email))
				D == None
			if D: 
				projects.append(D)
				for e in D.errors: 
					ERRORS.append(e)
	return projects

def _filter_exp_ids(r, start):
	exp_ids = []
	for exp_id in r.exp_descriptions.keys():
		earliest = datetime.datetime.strptime(r.exp_descriptions[exp_id]['earliest'][0:-1], "%Y-%m-%dT%H:%M:%S").date()
		if earliest > start.date():
			exp_ids.append(exp_id)
	return exp_ids

def _increment(count_hash, key, inc , ok_to_increment=True):
	if ok_to_increment:
		count_hash[key] = count_hash[key] + inc if key in count_hash else 1


def _start_date(r, exp_id):
	return datetime.datetime.strptime(r.exp_descriptions[exp_id]['earliest'][0:-1], "%Y-%m-%dT%H:%M:%S").date()

def _latest(r, exp_id):
	## Latest = "" means its running and will return an error. must decide what you will do with this. 
	try:
		latest = datetime.datetime.strptime(r.exp_descriptions[exp_id]['latest'][0:-1], "%Y-%m-%dT%H:%M:%S")
	except: 
		if r.exp_descriptions[exp_id]['status'] != "Running":
			print "ERROR: not running, but can't find end date of experiment: account_id, exp_id:", r.account_id, exp_id 
		else:
			account = s.query(Summary).filter(Summary.account_id == r.account_id).all()[0] # AIRBNB, use for goal checking 
			latest = datetime.date.today() if account.end_date == None else account.end_date
	return latest

def _countExp(r, exp_id):
	winning_experiment_count = 0 # experiments with a winning variation 
	winning_goal_count = 0 # goals that have a winning variation
	losing_experiment_count = 0 # experiments with a losing variation 
	losing_goal_count = 0 # goals that are losing
	pos_undecided_experiment_count = 0
	neg_undecided_experiment_count = 0
	
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
				if imp == "-" or CTB == "-":
					continue
				elif CTB > .95:
					print imp, CTB
					winning_variations += 1 
				elif CTB < .05: 
					losing_variations += 1
				elif imp > 0:
					pos_undecided_goals += 1
				elif imp < 0:
					neg_undecided_goals += 1
		print ".............. (winning_experiment_count, winning_goal_count, losing_experiment_count, losing_goal_count, pos_undecided_experiment_count, neg_undecided_experiment_count)", (winning_experiment_count, winning_goal_count, losing_experiment_count, losing_goal_count, pos_undecided_experiment_count, neg_undecided_experiment_count)
		if winning_variations > 0: 
			# Winner has been declared for a goal
			print "..............Adding winner"
			if not experiment_winner_counted:
				winning_experiment_count += 1
			# _increment(winning_experiment_count, (r.account_id, latest.year, latest.month), 1, not experiment_winner_counted)
			experiment_winner_counted = True
			if not goal_winner_declared:
				winning_goal_count += 1
			# _increment(winning_goal_count, (r.account_id, latest.year, latest.month), 1, not goal_winner_declared)
			goal_winner_declared = True
		elif losing_variations > 0:
			# Loser has been declared for a goal
			print "..............Adding loser"
			if not experiment_loser_counted:
				losing_experiment_count += 1 
			# _increment(losing_experiment_count, (r.account_id, latest.year, latest.month), 1, not experiment_loser_counted)
			experiment_loser_counted = True
			if not goal_loser_declared:
				losing_goal_count += 1
			# _increment(losing_goal_count, (r.account_id, latest.year, latest.month), 1, not goal_loser_declared)
			goal_loser_declared = True
	# The entire Experiment has no winning or losing variations, however, there are variations with improvement > 0
	if experiment_winner_counted is False and experiment_loser_counted is False: 
		print "..............Adding goals"
		if pos_undecided_goals > 0: 
			pos_undecided_experiment_count += 1
			# _increment(pos_undecided_experiment_count, (r.account_id, latest.year, latest.month), 1)
		elif neg_undecided_goals > 0: 
			neg_undecided_experiment_count += 1
			# _increment(neg_undecided_experiment_count, (r.account_id, latest.year, latest.month), 1)
	return (winning_experiment_count, winning_goal_count, losing_experiment_count, losing_goal_count, pos_undecided_experiment_count, neg_undecided_experiment_count)


#  TODO add a column for Running, and enter date it was running as of. 

def _updateAppUsage(r, filtered_exp_ids, account):
	date_range = [(start + relativedelta(months=i)) for i in range(0,13)]
	usage_arr = []
	for date in date_range:
		account.end_date = datetime.date.today() if account.end_date == None else account.end_date 
		if ((date.month < account.start_date.month and date.year == account.start_date.year) or date.year < account.start_date.year) or (date.year > account.end_date.year or (date.month > account.end_date.month and date.year == account.end_date.year)):
			 continue
		usage = AppUsage()
		goals_sum = 0
		exp_sum = 0
		for exp_id in filtered_exp_ids:
			start_date =  datetime.datetime.strptime(r.exp_descriptions[exp_id]['earliest'][0:-1], "%Y-%m-%dT%H:%M:%S")
			if (start_date.month == date.month and start_date.year == date.year): 
				goals_sum += len(r.goals[exp_id]['goals'])
				exp_sum += 1
		usage.month = date.month
		usage.poc = account.num_overall_pocs > 0
		usage.churn = account.churn 
		usage.year = date.year 
		usage.account_id = r.account_id 
		usage.avg_goals_per_experiment = goals_sum / float(exp_sum) if exp_sum > 0 else 0
		usage.num_experiments_started = exp_sum
		usage_arr.append(usage)
		s.merge(usage)

def dbg(dbobj, flat=False):
	object_list = [dbobj] if type(dbobj) is not types.ListType else dbobj
	attrs = engine.execute("select * from " + dbobj[0].__tablename__).keys()
	for obj in object_list: 
		if not flat:
			pprint([(attr, getattr(obj, attr)) for attr in attrs])
		else:
			print([(attr, getattr(obj, attr)) for attr in attrs])

start_time = datetime.datetime.now()
EXP_ERRORS = []

i = 1 
for account in accounts:
	print "ACCOUNT ", i, " of", len(accounts)
	i += 1 
	result_attrs =  [
		'exps_w_win_vars', 
		'goals_w_win_vars', 
		'exps_w_lose_vars',
		'goals_w_lose_vars',
		'win_undecided_exp',
		'lose_undecided_exp' ]
	
	try:
		email = _getImpersonationEmail(account)
	except Exception as e:
		print "ERROR: IMPERSONATION EMAIL account_id:", account.account_id, "error_type:", e
	try: 
		projects = _getProjects(account.account_id, email)
		if projects == []:
			print "ERROR: EmptyAccount account_id, email", account.account_id, email
	except Exception as e:
		print "ERROR: Exporting Results for account_id, email", account.account_id, email
	for r in projects: # r stands for results
		entries = []
		r.goal_names = r.setGoalNames()
		filtered_exp_ids = _filter_exp_ids(r, start)
		for exp_id in filtered_exp_ids:
			try:
				exp_start_date = _start_date(r, exp_id)
				r_entry = [e for e in entries if (e.year == exp_start_date.year and e.month == exp_start_date.month)]
				if r_entry == []:
					r_entry = Results()
					r_entry.account_id, r_entry.year, r_entry.month, r_entry.account_name = account.account_id, exp_start_date.year, exp_start_date.month, account.name
					entries.append(r_entry)
				else:
					r_entry = r_entry[0]
				
				for attr, incr_value in zip(result_attrs, _countExp(r, exp_id)):
					setattr(r_entry, attr, ((getattr(r_entry, attr) or 0) + incr_value))
			except Exception as e: 
				EXP_ERRORS.append(("Experiment Error, ", r.account_id, r.project_id, exp_id,  str(e)))
		try:
			_updateAppUsage(r, filtered_exp_ids, account)
		except Exception as e:
			EXP_ERRORS.append(("App Usage Error, ", r.account_id, r.project_id,  str(e)))
		
		[s.merge(e) for e in entries]
		s.commit()

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
		
account = s.query(Summary).filter(Summary.account_id == '223877531').all()[0] # Hubzu (highly active user)
email = _getImpersonationEmail(account)
account_id, project_id = '223877531', '341631422'
goal_id = '522040008'
exp_id = '525100008'
var_id = '521580011'
# ERRORS
# AccountID has nothing in it 
account = s.query(Summary).filter(Summary.account_id == '253274484').all()[0] # movember (highly active user)
account_id = '253274484'
project_id = '253274484'
email = _getImpersonationEmail(account)
projects = _getProjects(account.accountID, email)
r = projects[2]
r.goal_names = r.setGoalNames()
exp_id = '497430462'
_countExp(r, exp_id) == ({}, {}, {(u'223877531', 2014, 2): 1}, {(u'223877531', 2014, 2): 2}, {}, {})

account = s.query(Summary).filter(Summary.account_id == '236170800').all()[0] # AIRBNB, use for goal checking 
email = _getImpersonationEmail(account)
projects = _getProjects(account.account_id, email)
r = projects[0]
r.goal_names = r.setGoalNames() 
_countExp(r, '242732031') == (1, 1, 0, 0, 0, 0)
_countExp(r, '332707498') == (1, 1, 0, 0, 0, 0) # get the count right here. 
_countExp(r, '338428290') == (0, 0, 0, 0, 1, 0)
## Having Issues in STATS for the process that was ignored. must rerun requests for those processes. Hold everything in an error array. 



account = s.query(Summary).filter(Summary.account_id == '351480257').all()[0] # Kaiser
accounts = accounts[0]
email = _getImpersonationEmail(account)
projects = _getProjects(account.account_id, email)
r = projects[0]
r.goal_names = r.setGoalNames() 
