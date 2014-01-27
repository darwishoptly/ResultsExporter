#FINAL!!!!

import xlsxwriter
import requests
from bs4 import BeautifulSoup
from pprint import pprint 
import NormDist
import ExpDist
import OptlyData
import datetime
import operator

from multiprocessing import Process, JoinableQueue
from Queue import Empty

GAE_Auth_Cookie = "AJKiYcG35Lq3IdenWyKLED8qGQ2MpSaQV1qpvHcjynFPtHuBWd5JHZcmTDnoQ6oYVbFHytBRY1PSYSoYMeGotgrKUkUuyxxJAXVr62vpvm6mjtgEgKIVRJFsEkrG8Ize35OOxD04G1A4JwcksKLtrTJknRR59CvyDZBlp4xeeIT9SYU-msHye-MJHK6RhgcZSXVC667uyWsSUgefbipSg5wzLHMPbMlMtekbVzgQh0tWiaQT3GJ49SV0uYCZLGXf1zmuawfojES9pZryFAT-0kxEAaiEsE2aSoiFxuu-Yhs5r4nLMcCaryaE-muyq4DPV-mZICngneLuRk6RhOoz8Ad4_25l-dhM2HR9fA0CcD9iCb4QrkQ7sXtYj7MGpAfJNkb6ENt4noqZ-pb2WKqRldUORiAPYtmOfhuZap9yTsWVbwCPTDAmhFcal2rdkORkYNyrqDILki7f9h3qxxprqoRy0k9a_09736b_lQK6bA4-XLLI8XmWdNl9i9dh3kGPTWj4GYSL6OFNC3jmEu_A8DyCxWYcQ_QXY5rB8tLGNdbx06d15eaD4F78eOYVEuD17owIfvVLdce39aIOkBw_qd5s9m17gUmwcdx4GhHMaN8ylxv0BMnhCe4y2pqYMtvIDxHdtor2t8cFU8t4LBX_i1WJnWowEIH-5Q"
# optimizely_session = "fc7393a777c80660fb925303329039693509f167"
email = "jlee@travelzoo.com"
a_name = "Travel Zoo"

# email = "mjolley@fareportal.com"
project_id = 93281222
account_id = 93281222
name = a_name+"_%s.xlsx" % str(datetime.date.today())

D = OptlyData.client(GAE_Auth_Cookie, project_id, account_id, {"start": True, "email": email})
D.setGoalNames()

num_experiments = len(D.exp_descriptions.keys())

segment_names = {}
for segment in requests.get("https://www.optimizely.com/api/projects/%s/segments.json?default_segments=true&token=%s" % (str(project_id), D.account_token), cookies={"optimizely_session": D.optimizely_session, "SACSID" : D.GAE_Auth_Cookie}).json()["segments"]:
	segment_names[str(segment['id'])] = segment["name"]

segment_value_maps = {} # exp: seg value map
for exp_id in D.exp_descriptions.keys():
	print "....segment_value_maps.......", exp_id
	segment_value_maps[exp_id] = requests.get("https://api.optimizely.com/v1/segment_values/%s?token=%s" % (str(exp_id), D.token_hash[exp_id]), cookies={"optimizely_session": D.optimizely_session, "SACSID" : D.GAE_Auth_Cookie}).json()["segment_value_map"]

#SCHEMA ==> { s_id : {s_val : 0, count : 1 } }
# Create Segment ID, Val Pairs with frequency
segment_id_value_pairs = {}
for exp_id in D.exp_descriptions.keys():
	for s_id in segment_value_maps[exp_id]:
		for s_val in segment_value_maps[exp_id][s_id]:
			if (s_id, s_val) in segment_id_value_pairs:
				segment_id_value_pairs[(s_id, s_val)] += 1 	 
			else: 
				segment_id_value_pairs[(s_id, s_val)] = 1

sorted_seg_pairs = sorted(segment_id_value_pairs.items(), key= lambda x: x[1], reverse=True) 
## Filter out segment pairs that are not in atleast 25% of all experiments. 
sorted_seg_pairs = [pair for pair in sorted_seg_pairs if float(pair[1]) / float(num_experiments) > .25]

S = []
i = 1
# pair = ('167439469'  ,  'unknown')
for pair in sorted_seg_pairs:
	# print D.exp_descriptions
	pair = pair[0] 
	print "PAIR: ", i, " out of" , str(len(sorted_seg_pairs)), pair[0], " : " , pair[1], "........", i
	s = OptlyData.client(GAE_Auth_Cookie, project_id, account_id, { "optimizely_session": D.optimizely_session, 
													 				"segment_id" : pair[0],
																	"segment_value": pair[1],  
																	"segment_value_maps": segment_value_maps,
																	"token_hash" : D.token_hash,
																	"start" : False,
																	"exp_descriptions": D.exp_descriptions.copy(), 
																	"account_token": D.account_token,
																	"D": D
																	})
	s.setVisitorCount()
	s.setGoals()
	s.setResultStatistics()
	S.append(s)
	i = i + 1

 
# summary_num = 15
# num_important = 3
# common_goals = D.createGoalCount()
# 
# num_high = 0
# num_low = 0
# imp_goals_positive = {}
# imp_goals_negative = {}
# for g in common_goals:
# 	imp_goals_positive[g[0]] = []
# 	imp_goals_negative[g[0]] = []
# 
# def insertIfImportant(goal_id, exp_id, var_id, improvement):
# 	# Assumes largest first, smallest lastom
# 	if improvement > 0 and sum([len(imp_goals_positive[x]) for x in imp_goals_positive]) < summary_num:
# 		if exp_id == "381700640" and goal_id == "231307144":
# 			print goal_id, exp_id, var_id, improvement		
# 		imp_goals_positive[goal_id].append({ "exp_id" : exp_id, "var_id": var_id, "improvement" : improvement })
# 		imp_goals_positive[goal_id] = sorted(imp_goals_positive[goal_id], key= lambda k: k['improvement'] , reverse=True)
# 		if(len(imp_goals_positive[goal_id]) > num_important):
# 			imp_goals_positive[goal_id].pop()
# 	elif sum([len(imp_goals_negative[x]) for x in imp_goals_negative]) < summary_num:
# 		# Assumes smallest first, largest last	
# 		imp_goals_negative[goal_id].append({ "exp_id" : exp_id, "var_id": var_id, "improvement" : improvement })
# 		imp_goals_negative[goal_id] = sorted(imp_goals_negative[goal_id], key= lambda k: k['improvement'])
# 		if(len(imp_goals_negative[goal_id]) > num_important):
# 			imp_goals_negative[goal_id].pop()
# 
# exp_ids = D.exp_descriptions.keys()
# for g in common_goals:
# 	goal_id = g[0]
# 	for exp_id in exp_ids:
# 		if goal_id in D.goals[exp_id]['goals']:
# 			for var_id in D.visitor_count[exp_id]['variation'].keys():
# 				if exp_id not in D.goals or var_id not in D.variation_names:
# 					print "skipping for most imp: ", exp_id, goal_id, var_id 
# 					continue
# 				imp = D.goals[exp_id]['goals'][goal_id][var_id]['improvement']				
# 				if imp == "-" or D.goals[exp_id]['goals'][goal_id][var_id]['conversions'] < 100:
# 					continue
# 				else:
# 					# print ".... inserting goal .....", goal_id, exp_id, var_id
# 					insertIfImportant(goal_id, exp_id, var_id, float(imp))
# 					if imp > .05:
# 						num_high +=1 
# 					if imp < -.05: 
# 						num_low += 1 
# 
# #  remove duplicates 
# for g_id in imp_goals_positive.keys(): 
# 	for exp in imp_goals_positive[g_id]:
# 		if exp in imp_goals_negative[g_id]: 
# 			if exp['improvement'] > 0:
# 				imp_goals_negative[g_id].remove(exp)
# 			else:
# 				imp_goals_positive[g_id].remove(exp)
# 
# for goal_id in imp_goals_positive.keys():
# 	if imp_goals_positive[goal_id] == []:
# 		del imp_goals_positive[goal_id]
# 	elif imp_goals_negative[goal_id] == []:
# 		del imp_goals_negative[goal_id]



i = 0
imp_goals_positive = []
imp_goals_negative = []
summary_num = 15
num_important = 3
common_goals = D.createGoalCount()

while i < len(common_goals):
	goal_id, goal_count = common_goals[i][0], common_goals[i][1]
	goal_ids = [goal_id]
	positive = []
	negative = []
	while i < len(common_goals) - 2 and common_goals[i + 1][1] == goal_count:
		i += 1
		goal_ids.append(common_goals[i][0])
	# print goal_ids
	for g_id in goal_ids:
		# print g_id
		for exp_id in exp_ids:
			if g_id in D.goals[exp_id]['goals']:
				for var_id in D.visitor_count[exp_id]['variation'].keys():
					if exp_id not in D.goals or var_id not in D.variation_names:
						print "skipping for most imp: ", exp_id, g_id, var_id 
						continue
					imp = D.goals[exp_id]['goals'][g_id][var_id]['improvement']	
					baseline_id = D.exp_descriptions[exp_id]['baseline_id']
					b_conversions = D.goals[exp_id]['goals'][g_id][baseline_id]['conversions']			
					if imp == "-" or D.goals[exp_id]['goals'][g_id][var_id]['conversions'] < 100 or b_conversions < 100:
						continue
					elif imp > 0:
						# print {"goal_id" : g_id,  "exp_id" : exp_id, "var_id": var_id, "improvement" : imp }
						positive.append({"goal_id" : g_id,  "exp_id" : exp_id, "var_id": var_id, "improvement" : imp })
					elif imp < 0:	
						negative.append({"goal_id" : g_id, "exp_id" : exp_id, "var_id": var_id, "improvement" : imp })
	imp_goals_positive += sorted(positive, key= lambda x: x["improvement"], reverse=True)[0:3]
	imp_goals_negative += sorted(negative, key= lambda x: x["improvement"])[0:3]
	i+=1

while len(imp_goals_positive) > summary_num:
	imp_goals_positive.pop()

while len(imp_goals_negative) > summary_num:
	imp_goals_negative.pop()	

num_high = 0
num_low = 0		
for exp_id in D.goals:
	plus_low, plus_high = True, True
	for goal_id in D.goals[exp_id]['goals']:
		for var_id in D.visitor_count[exp_id]['variation'].keys():
			if exp_id not in D.goals or var_id not in D.variation_names:
				print "skipping for most imp: ", exp_id, g_id, var_id 
				continue
			imp = D.goals[exp_id]['goals'][goal_id][var_id]['improvement']
			baseline_id = D.exp_descriptions[exp_id]['baseline_id']
			b_conversions = D.goals[exp_id]['goals'][goal_id][baseline_id]['conversions']	 
			# print imp, D.goals[exp_id]['goals'][goal_id][var_id]['conversions'] < 50, b_conversions < 50
			if imp == "-" or D.goals[exp_id]['goals'][goal_id][var_id]['conversions'] < 50 or b_conversions < 50:
				continue
			elif imp > .05 and plus:
				num_high += 1
				plus_high = False
			elif imp < -.05 and plus:
				num_low += 1
				print exp_id, goal_id, var_id, D.goals[exp_id]['goals'][goal_id][var_id]['conversions'], b_conversions, imp
				plus_low = False	

## Segments
# Ignore Segments that are only applicable to one test
# Consider looking only at segment pairs that are in atleast 30% of experiments (Speeds things up a lot) + have over X number of visitors compared to the full experiment! 
# Ignore experiments with a small number of visitors. 

def removeLowExpVisitors(s, percent): 
	for exp_id in s.exp_descriptions.keys():
		total_visitors = D.visitor_count[exp_id]["total_visitors"]
		if s.visitor_count[exp_id]["total_visitors"] < percent * total_visitors: 
			del s.exp_descriptions[exp_id]
			del s.visitor_count[exp_id]

[removeLowExpVisitors(s, .05) for s in S]
S = ([t for t in S if len(t.visitor_count) > 1])

def segmentWeight(s):
	seg_visits = 0
	total_visits = 0	
	for exp_id in s.exp_descriptions.keys():
		seg_visits += s.visitor_count[exp_id]["total_visitors"]  
		total_visits += D.visitor_count[exp_id]["total_visitors"]
	print s.segment_id, s.segment_value, seg_visits, total_visits
	return float(seg_visits) / total_visits

deviant_segments = []
for s in S: 
	for exp_id in s.goals.keys():
		for goal_id in s.goals[exp_id]["goals"].keys():
			for var_id in s.goals[exp_id]["goals"][goal_id].keys():					
				if var_id not in D.variation_names or var_id == 'name' or s.goals[exp_id]["goals"][goal_id][var_id]["improvement"] == "-":
					continue
				else: 
					seg = s.goals[exp_id]["goals"][goal_id][var_id]["improvement"]
					original = D.goals[exp_id]["goals"][goal_id][var_id]["improvement"]
					difference = seg - original 
					conversions = s.goals[exp_id]["goals"][goal_id][var_id]["conversions"]
					if abs(difference) > .1 and conversions > 250: 
						deviant_segments.append((s.segment_id, s.segment_value, goal_id, exp_id, var_id, float("{0:.2f}".format(difference)), segmentWeight(s) ))


deviant_segments = sorted(deviant_segments, key= lambda k: k[5], reverse=True)


count_deviant_segments = {}
for s in deviant_segments:
	if (s[0], s[1]) in count_deviant_segments:
		if s[3] not in count_deviant_segments[(s[0], s[1])]["exp_ids"]:
			count_deviant_segments[(s[0], s[1])]["count"] += 1
			count_deviant_segments[(s[0], s[1])]["exp_ids"].append(s[3])
	else:
		count_deviant_segments[(s[0], s[1])] = { "count" : 1, "exp_ids" : [s[3]], "weight": s[6]}

for s in count_deviant_segments:
	i, v = s[0], s[1]
	count_deviant_segments[(i, v)]["weighted_count"] = float(count_deviant_segments[(i, v)]["count"]) *  count_deviant_segments[(i, v)]["weight"]

t = sorted(count_deviant_segments.items(), key = lambda x: x[1]["count"], reverse=True)
t2 = sorted(count_deviant_segments.items(), key = lambda x: x[1]["weight"], reverse=True)
t3 = sorted(count_deviant_segments.items(), key = lambda x: x[1]["weighted_count"], reverse=True)
# [(i[0][0], i[0][1]) for i in t3]

# highlight where goal is a "top 5 " goal
# [ e for e in D.exp_descriptions if datetime.datetime.strptime(D.exp_descriptions[e]['last_modified'][0:-1] , "%Y-%m-%dT%H:%M:%S") > datetime.datetime(2014,1,1,1,1,1)]

def writeRange(worksheet, row, start_col, values, update=False, formats = []):
    global col
    i = 0 
    for v in values:
        if len(formats) > 0:
            f = formats[i]
            i+=1
        else:
            f = workbook.add_format({});
        worksheet.write(row, start_col, v, f)
        start_col += 1
    if update:
        col = start_col
    return

def addSpaceColumn(worksheet, column):
    worksheet.set_column(column, column, 2)
    global col
    col += 1
    return 

def setGoalHeaders(worksheet, row, column, goals):
    global col
    col = column
    for goal in goal_count:            
        goal_id = goal[0]
        if goal_id in goals:
            # merge the length of number of goal fields - 1
            addSpaceColumn(worksheet, col)
            f = combinedFormat(["center", "bottom", "font"])
            worksheet.merge_range(row, col, row, col + 4 - 1, goal_names[goal_id], workbook.add_format(f))
            col+=4
    return 

min_visits = 1000
goals_with_min_visits = sum([1 for exp in D.visitor_count if D.visitor_count[exp]['total_visitors'] > 1000])
avg_goals_exp = float(sum([len(D.goals[exp_id]['goals']) for exp_id in D.goals if D.visitor_count[exp_id]['total_visitors'] > 1000])) / goals_with_min_visits

workbook = xlsxwriter.Workbook(name);
summary_sheet = workbook.add_worksheet("Summary")
segment_sheet = workbook.add_worksheet("Segments")
worksheet = workbook.add_worksheet("Experiments Report - Detailed")
dump = workbook.add_worksheet("Dump")

formats = {
    "percent" : {'num_format': '0.0%'}, 
    "decimal" : {'num_format': '0.00'}, 
    'fill_green': {'bg_color': '#E7EFD8'},
    "fill_red" : {"bg_color" : '#F2DCDB'},
    "center": {'align': 'center'},
    "bottom": {'bottom':1},
    "font": {"font_name": "Gill Sans Light"},
    "font_bold": {"font_name": "Gill Sans"},
    "wrap": {"text_wrap": "True"},
    "fill_grey": {'bg_color': '#D9D9D9'},
	"strong_bold": {"bold": "true"},
	"bottom_heavy": {"bottom": 5},
	"top" : {"top": 1},
	"v_middle" : {"valign": "vcenter"},
	"": {"font_name": "Gill Sans Light"}
    }

def combinedFormat(list_formats): # ["percent", "fill_green"]
    combined_format = {}
    for f in list_formats: combined_format.update(formats[f])
    return combined_format

def getGoalFormats(goal_name, improvement):
    if improvement > .05 and improvement != "-": 
        percentage = workbook.add_format(combinedFormat(["percent", "fill_green", "font"]))
        decimal = workbook.add_format(combinedFormat(["decimal", "fill_green", "font"]))
    elif improvement < -.05 and improvement != "-":
        percentage = workbook.add_format(combinedFormat(["percent", "fill_red", "font"]))
        decimal = workbook.add_format(combinedFormat(["decimal", "fill_red", "font"])) 
    else:
        percentage = workbook.add_format(combinedFormat(["percent", "font"]))
        decimal = workbook.add_format(combinedFormat(["decimal", "font"])) 
    if goal_name == "Total Revenue":
        return [decimal, decimal, percentage, percentage]
    else:
        return [decimal, percentage, percentage, percentage]

row, col = 1, 1
segment_sheet.set_column(0, 0, 1)
segment_sheet.set_column(1, 11, 18)
segment_sheet.set_column(4, 6, 25)
segment_sheet.set_column(7, 11, 13)
segment_sheet.hide_gridlines(2)
segment_sheet.set_zoom(90)

empty = ["" for i in range(10)]
f = workbook.add_format(combinedFormat(["font_bold", "strong_bold", "bottom_heavy"]))
f_mats = [f for i in range(11)]
writeRange(segment_sheet, row, col, [a_name.upper() + " SEGMENT DISCOVERY"] + empty, False, f_mats)
f = workbook.add_format(combinedFormat(["font_bold", "bottom"]))
f_mats = [f for i in range(12)]
headers = ["Experiment ID",
		   "Segment Name",	
		   "Segment Value",	
		   "Experiment Name",
		   	"Variation Name",	
			"Goal Name",
			"Exp Visitors",	
			"Segment Visitors",	
			"Org. Improvement",	
			"+/- Improvement",	
			"Segment Weight"]
row += 2
writeRange(segment_sheet, row, col, headers, False, f_mats)
row+=1
for d in deviant_segments:
	exp_id, var_id, goal_id = d[3], d[4], d[2]  
	values = [
		d[3],
		segment_names[d[0]],
		d[1],
		D.exp_descriptions[d[3]]['description'],
		D.variation_names[d[4]],
		D.goal_names[d[2]],
		D.visitor_count[exp_id]["variation"][var_id],
		"TBD",
		D.goals[exp_id]["goals"][goal_id][var_id]["improvement"],
		d[5],
		"TBD" 
	]
	imp = "fill_green" if goal_id in imp_goals_positive else "" # short for importance
	tex = workbook.add_format(combinedFormat(["font", "v_middle", imp]))
	tex_c = workbook.add_format(combinedFormat(["font", "v_middle", "center", imp]))
	per = workbook.add_format(combinedFormat(["font", "v_middle", "percent", "center", imp]))
	dec = workbook.add_format(combinedFormat(["font", "v_middle", "decimal", "center", imp]))
	f_mats = [tex,tex, tex, tex, tex, tex, tex_c, tex_c, per, dec, tex_c ]
	writeRange(segment_sheet, row, col, values, False, f_mats)
	row += 1

row, col = 1, 1  
summary_sheet.set_column(0, 0, 1)
summary_sheet.set_column(1, 4, 18)
summary_sheet.set_column(5, 5, 1)
summary_sheet.set_column(6, 9, 18)
summary_sheet.set_column(10, 10, 1)
summary_sheet.set_column(11, 14, 18) 
summary_sheet.hide_gridlines(2)
summary_sheet.set_zoom(90)
   
#Main Header and High Level Stats 
empty = ["" for i in range(13)]
f = workbook.add_format(combinedFormat(["font_bold", "strong_bold", "bottom_heavy"]))
f_mats = [f for i in range(14)]
writeRange(summary_sheet, row, col, [a_name.upper() + " EXECUTIVE DASHBOARD"] + empty, False, f_mats)
f = workbook.add_format(combinedFormat(["font"]))
num = workbook.add_format(combinedFormat(["decimal", "font"]))
row, col = 2, 1
writeRange(summary_sheet, row, col, ["Date Run", "", "", str(datetime.date.today())], False, [f,f,f,f])
row, col = row + 1, 1
writeRange(summary_sheet, row, col, ["Criteria", "", "", "Modified in Last 90 days"], False, [f,f,f,f])
row, col = row + 2, 1
f = workbook.add_format(combinedFormat(["font"]))
num = workbook.add_format(combinedFormat(["decimal", "font"]))
stats = [("# Experiments Run w/ >%s visitors" % min_visits , goals_with_min_visits), ("Avg. Goals / Experiment", avg_goals_exp), ("# Experiments with Improvement > .05", num_high), ("# Experiments with Improvement< -.05", num_low) ]
for s in stats:
	writeRange(summary_sheet, row, col, [s[0], "", "", s[1]], False, [f,f,f,num])
	row, col = row + 1, 1

row += 1
#Set Headers for Stats 
summary_sheet.merge_range(row, col, row, 9, "EXPERIMENT SUMMARY STATISTICS", workbook.add_format(combinedFormat(["font_bold", "bottom", "top", "center"])))
col += 9 + 1
summary_sheet.merge_range(row, col, row, col+4, "SEGMENT SUMMARY STATISTICS", workbook.add_format(combinedFormat(["font_bold", "bottom", "top", "center"])))
row, col = row + 1, 1
summary_sheet.merge_range(row, col, row, col+3, "High Improvement Variations for Frequent Goals:", workbook.add_format(combinedFormat(["font_bold", "fill_green", "center"])))
col += 5
summary_sheet.merge_range(row, col, row, col+3, "Low Improvement Variations for Frequent Goals:", workbook.add_format(combinedFormat(["font_bold", "fill_red", "center"])))
col += 5 
summary_sheet.merge_range(row, col, row, col+4, "Segment Value Pairs w/ >10% deviation :", workbook.add_format(combinedFormat(["font_bold", "fill_grey", "center"])))
row, col = row + 1, 1
f = workbook.add_format(combinedFormat(["font", "bottom", "center"]))
for i in range(2):
	writeRange(summary_sheet, row, col, ["Experiment Name", " Variation", "Improvement", "Goal Name"], False, [f,f,f,f])
	col+=5	

writeRange(summary_sheet, row, col, ["Segment Name", "Segment Value", "# Expmts with deviation", "Avg % Total Visitors" , "Score"], False, [f,f,f,f,f])


# Top 5 Imp Goals 
# row, col= row + 1, 1 
# data_start_row = row 
# for important_set in [imp_goals_positive, imp_goals_negative]:
# 	for goal_id in important_set:
# 		for v in  important_set[goal_id]:
# 			f = workbook.add_format(combinedFormat(["font", "wrap", "v_middle"]))
# 			p = workbook.add_format(combinedFormat(["percent", "font", "center", "v_middle"]))
# 			values = [D.exp_descriptions[v["exp_id"]]["description"], D.variation_names[v["var_id"]], v["improvement"], D.goal_names[goal_id]]
# 			writeRange(summary_sheet, row, col, values, False, [f, f, p, f])
# 			row += 1
# 	row, col = data_start_row, col + 5

row, col= row + 1, 1 
data_start_row = row 
for important_set in [imp_goals_positive, imp_goals_negative]:
	for goal in important_set:
		f = workbook.add_format(combinedFormat(["font", "wrap", "v_middle"]))
		p = workbook.add_format(combinedFormat(["percent", "font", "center", "v_middle"]))
		values = [D.exp_descriptions[goal["exp_id"]]["description"], D.variation_names[goal["var_id"]], goal["improvement"], D.goal_names[goal["goal_id"]]]
		writeRange(summary_sheet, row, col, values, False, [f, f, p, f])
		row += 1
	row, col = data_start_row, col + 5

# important_segments = sorted(count_deviant_segments.items(), key = lambda x: x[1], reverse=True)
for s in t3[0:15]: # TODO change name 
	f = workbook.add_format(combinedFormat(["font", "wrap", "v_middle"]))
	n = workbook.add_format(combinedFormat(["font", "wrap", "center", "v_middle"]))
	p = workbook.add_format(combinedFormat(["font", "wrap", "center", "v_middle", "percent"]))
	d = workbook.add_format(combinedFormat(["font", "wrap", "center", "v_middle", "decimal"]))
	values = [segment_names[s[0][0]], s[0][1], s[1]["count"], s[1]["weight"], s[1]["weighted_count"] ]
	writeRange(summary_sheet, row, col, values, False, [f, f, n, d, d])
	row += 1

# percentage = workbook.add_format({'num_format': '0.0%'})
# two_digit_decimal = workbook.add_format({'num_format': '0.00'})
row, col = 0 , 0 

goal_count = D.createGoalCount()
D.setGoalNames()
goal_names = D.goal_names

## Set up Headers 
headers = ["Variation Name", "Visitors"]
f = workbook.add_format(combinedFormat(["font_bold", "fill_grey"]))
writeRange(worksheet, row, col, headers, True, [f,f])
addSpaceColumn(worksheet, col)
worksheet.set_column(0, 0, 30)
worksheet.set_column(1, 1, 12)
## Set up Goal Headers
fields = ["Conversions", "CNV Rate", "Improvement", "CTB"]
for i in range(0, D.maxGoals()): 
    col_old = col
    center = workbook.add_format(combinedFormat(["center", "font_bold", "fill_grey"]))
    writeRange(worksheet, row, col, fields, True, [center, center, center, center])
    worksheet.set_column(col_old, col, 12)
    addSpaceColumn(worksheet, col)

row += 2
col = 0

expIDSortedbyVisitorCount = sorted(D.visitor_count, key=lambda x: D.visitor_count[x]['total_visitors'], reverse=True)

for exp_id in expIDSortedbyVisitorCount:
# exp_id = '523462609'
	print "ADDING: Experiment ID: ", exp_id
	try: 
		D.visitor_count[exp_id]
	except:
		print ("ERROR:", exp_id, var_id)
		continue
	font = workbook.add_format(formats["font"])
	font_bold = workbook.add_format(combinedFormat(["font_bold", "wrap"]))
	worksheet.write(row, 0, D.exp_descriptions[exp_id]['description'] + " (" + str(exp_id) + ")", font_bold) 
	col+=2
	goal_ids = D.goals[exp_id]["goals"].keys()
	setGoalHeaders(worksheet, row, col, goal_ids)   
	row += 1
	# sort variations with baseline first 
	var_ids  = D.visitor_count[exp_id]['variation'].keys()
	baseline_variation_id = filter(lambda var_id: var_id == D.exp_descriptions[exp_id]["baseline_id"] , var_ids)    
	baseline_variation_id = baseline_variation_id[0] if len(baseline_variation_id) > 0 else var_ids[0]
	var_ids.remove(baseline_variation_id)
	var_ids.insert(0, baseline_variation_id)
	# Add Variation Names 
	for var_id in var_ids:
		col = 0
		if var_id not in D.variation_names:
			print "..........VARIATION DELETED........ ", var_id
			continue
		writeRange(worksheet, row, col, [D.variation_names[var_id], D.visitor_count[exp_id]['variation'][var_id]], True, [font, font])
		for goal in goal_count:            
			goal_id = goal[0]
			if goal_id in goal_ids:
				addSpaceColumn(worksheet, col)
				g = D.goals[exp_id]["goals"][goal_id][var_id]
				(conversions, conversion_rate, improvement, CTB) = g["conversions"], g["conversion_rate"], g["improvement"], g["CTB"] 
				goal_formats = getGoalFormats(D.goals[exp_id]["goals"][goal_id]["name"], improvement)
				writeRange(worksheet, row, col, [conversions, conversion_rate, improvement, CTB], True, goal_formats) 
		row += 1
	row += 1
	col = 0

f = workbook.add_format(combinedFormat(["font_bold", "fill_grey", "center"]))
headers = ["Experiment ID", 
		   "Variation ID", 
		   "Experiment Name", 
		   "Variation Name", 
		   "Conversions", 
		   "Conversion Rate", 
		   "Improvement", 
		   "CTB",
		   "Segment Name",
		   "Segment Value"]
f_mats = [f for h in headers]
writeRange(worksheet, row, col, headers, True, f_mats) 
for exp_id in D.goals:
	plus_low, plus_high = True, True
	for goal_id in D.goals[exp_id]['goals']:
		for var_id in D.visitor_count[exp_id]['variation'].keys():
			if exp_id not in D.goals or var_id not in D.variation_names:
				print "skipping for most imp: ", exp_id, g_id, var_id 
				continue
			seg_name = "Original" if D.segment_id == "" else ""
			r = [exp_id,
				var_id,
				D.exp_descriptions[exp_id]["description"],
				D.variation_names[var_id],
				D.goals[exp_id]['goals'][goal_id][var_id]["conversions"],
				D.goals[exp_id]['goals'][goal_id][var_id]["conversion_rate"],
				D.goals[exp_id]['goals'][goal_id][var_id]["Improvement"],
				D.goals[exp_id]['goals'][goal_id][var_id]["CTB"],
				seg_name,
				D.segment_value]
			text = workbook.add_format(combinedFormat(["font"]))
			num = workbook.add_format(combinedFormat(["decimal", "font"]))
			percent = workbook.add_format(combinedFormat(["format", "font"]))
			f_mats = [num,
					num,
					text,
					text, 
					num,
					percent,
					percent,
					percent,
					percent]
			writeRange()



top_row = workbook.add_format(combinedFormat(["font_bold", "fill_grey"]))
worksheet.set_row(0, None, top_row)
worksheet.set_zoom(75)
worksheet.freeze_panes(1,2)
worksheet.hide_gridlines(2)
workbook.close()



