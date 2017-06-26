from flask import jsonify
from flask import request
from flask import render_template
from app import app
from cassandra.cluster import Cluster
cluster = Cluster(['ec2-34-225-164-189.compute-1.amazonaws.com'])
session = cluster.connect('pageranks')

user_list = []

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
        return "2008:" + str(self.avg08) + "  " + "2009:" + str(self.avg09) + "  " + "2010:" + str(self.avg10) + "  " + \
         "2011:" + str(self.avg11) + "  " + "2012:" + str(self.avg12) + "  " + "2013:" + str(self.avg13) + "  " + \
         "2014:" + str(self.avg14) + "  " + "2015:" + str(self.avg15) + "  " + "2016:" + str(self.avg16) + "  " + \
         "2017:" + str(self.avg17)


@app.route('/')
@app.route('/index')
def index():
    return render_template("index.html", user_list = user_list)

@app.route('/test2')
def test2():
	return "test2 success"

@app.route('/index', methods=['POST'])
def handle_data():
    reddit_username = request.form['search']
    reddit_username = '\'' + reddit_username + '\''
    stmt = ''' SELECT * FROM allranks WHERE name=''' + reddit_username
    response = session.execute(stmt)
    response_list = []
    for val in response:
    	response_list.append(val)
    newuser = User(response_list[0].name, response_list[0].average_2008,
        response_list[0].average_2009, response_list[0].average_2010,
        response_list[0].average_2011, response_list[0].average_2012,
        response_list[0].average_2013, response_list[0].average_2014,
        response_list[0].average_2015, response_list[0].average_2016,
        response_list[0].average_2017)

    user_list.append(newuser)
    return render_template("index.html", user_list = user_list)