from flask import jsonify
from flask import request
from flask import render_template
from app import app
from cassandra.cluster import Cluster
cluster = Cluster(['ec2-34-225-164-189.compute-1.amazonaws.com'])
session = cluster.connect('pageranks')
session_subs = cluster.connect('reddit')

user_list = []
subreddit_list = []
year_list = ['2017', '2016', '2015', '2014', '2013', '2011', '2010', '2009', '2008']


class User(object):
    def __init__(self, name, avg08, avg09, avg10, avg11, avg12, avg13, avg14, avg15, avg16, avg17):
        self.name = name
        self.avg08 = avg08
        self.avg09 = avg09
        self.avg10 = avg10
        self.avg11 = avg11
        self.avg12 = avg12
        self.avg13 = avg13
        self.avg14 = avg14
        self.avg15 = avg15
        self.avg16 = avg16
        self.avg17 = avg17

    def string_out(self):
        return ["2008:    " + str(self.avg08), "2009:    " + str(self.avg09), "2010:    " + str(self.avg10),
         "2011:    " + str(self.avg11),"2012:    " + str(self.avg12), "2013:    " + str(self.avg13),
         "2014:    " + str(self.avg14),"2015:    " + str(self.avg15),"2016:    " + str(self.avg16),
         "2017:    " + str(self.avg17)]


class Subreddit(object):
    def __init__(self, name, adjacencies, year):
        self.name = name + " " + year
        self.adjacencies = sorted(adjacencies, key=lambda x:x[1])

    def get_similar(self):
        similar = []
        for adj, val in reversed(self.adjacencies):
            similar.append(adj)
        return similar


@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html", user_list = user_list)

@app.route('/index', methods=['POST'])
def handle_data():
    reddit_username = request.form['search']
    reddit_username = '\'' + reddit_username + '\''
    stmt = ''' SELECT * FROM allranks WHERE name=''' + reddit_username
    response = session.execute(stmt)
    response_list = []
    for val in response:
    	response_list.append(val)
    if (len(response_list) == 0):
        return render_template("index.html", user_list = user_list)
    newuser = User(response_list[0].name, response_list[0].average_2008,
        response_list[0].average_2009, response_list[0].average_2010,
        response_list[0].average_2011, response_list[0].average_2012,
        response_list[0].average_2013, response_list[0].average_2014,
        response_list[0].average_2015, response_list[0].average_2016,
        response_list[0].average_2017)
    user_list.insert(0, newuser)
    return render_template("index.html", user_list = user_list)

@app.route('/subreddit')
def subreddit():
    return render_template("subreddit.html", subreddit_list = subreddit_list)

@app.route('/subreddit', methods=['POST'])
def handle_subreddit_data():
    sub = request.form['search_subreddit'] + ''
    if (';' in sub):
        new_subreddit = Subreddit('Stop trying to SQL inject my demo', [], 'Tigran!')
        subreddit_list.insert(0, new_subreddit)
        return render_template("subreddit.html", subreddit_list = subreddit_list)
    year = sub[:4]
    s_reddit = sub[5:]
    if (year not in year_list):
        new_subreddit = Subreddit('Stop trying to break my demo Tigran.', [], 'Queries must be in the form \'year:Subreddit\' or \'year Subreddit\'')
        subreddit_list.insert(0, new_subreddit)
        return render_template("subreddit.html", subreddit_list = subreddit_list)

    s_reddit = '\'' + s_reddit + '\''
    stmt = ''' SELECT *  FROM ''' + '''subs_''' + year + ''' WHERE name=''' + s_reddit
    response = session_subs.execute(stmt)
    response_list = []

    for val in response:
        response_list.append(val)
    if (len(response_list) == 0):
        return render_template("subreddit.html", subreddit_list = subreddit_list)

    new_adjacencies = []
    for item in response_list[0].adj:
        new_adjacencies.append((item, response_list[0].adj.get(item)))

    new_subreddit = Subreddit(response_list[0].name, new_adjacencies, year)
    subreddit_list.insert(0, new_subreddit)
    return render_template("subreddit.html", subreddit_list = subreddit_list)

