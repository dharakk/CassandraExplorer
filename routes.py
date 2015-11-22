import json

from cassandra.cluster import Cluster

from flask import Flask
from flask import render_template, request, redirect, url_for

app = Flask(__name__)



keysp = "universe"
column_family = "movies"
nocols = 4;

cluster = None;
@app.route("/cexplore")
def main():
    return render_template('index.html')

@app.route("/cexplore/query", methods=['POST'])
def query_result():
	session = cluster.connect()
	
	#print info
	headers = [];
	selects = "";
	req = request.form;
	for i in range(1,nocols+1):
		s ="checkboxes"+str(i) 
		if s in req:
			headers.append(req[s]);

	selects = ",".join(headers);

	info = session.execute("SELECT "+selects+" FROM "+keysp+"."+column_family)
	print info
	rows = [] 
	for x in info:
		row=[];
		for y in x:
			row.append(y);
		rows.append(row);
	return render_template('show_result.html',rows = rows,ths = headers,colfam = column_family)

@app.route("/cexplore/try_connect",methods=['POST'])
def try_connect():
	global cluster
	ip = request.form['textinput']
	cluster = Cluster([ip])
	try:
		session = cluster.connect();
	except:
		return redirect('cexplore/error')	
	return redirect('cexplore/listkeysp')


@app.route("/cexplore/keyspops")
def keyspace_options():
	return render_template('keyspace_ops.html');

@app.route("/cexplore/create")
def create_input():
	return render_template('create_keyspace.html');

@app.route("/cexplore/drop")
def drop_input():
	return render_template('drop_keyspace.html');

@app.route("/cexplore/colops")
def col_options():
	global keysp, column_family
	keysp = request.args['q'];
	return render_template('column_ops.html',keysp=keysp);



@app.route("/cexplore/create_keysp", methods=['POST'])
def create_keyspace():
	global keysp, column_family
	tmp = request.form['textinput'];
	session = cluster.connect()
	
	query = "CREATE KEYSPACE " + tmp + " WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3}"
	session.execute(query)
	keysp = tmp
	return render_template('column_ops.html',keysp=keysp);

@app.route("/cexplore/drop_keysp", methods=['POST'])
def drop_keyspace():
	global keysp, column_family
	tmp = request.form['textinput'];
	session = cluster.connect()
	query = "DROP KEYSPACE " + tmp
	#query = "CREATE KEYSPACE " + tmp + " WITH replication = {'class' : 'SimpleStrategy', 'replication_factor' : 3}"
	session.execute(query)
	
	return redirect('cexplore/listkeysp');


@app.route("/cexplore/listkeysp")
def listkeyspaces():
	global keysp, column_family
	
	session = cluster.connect()
	query = "select keyspace_name from system.schema_keyspaces;"
	print query
	info = session.execute(query)
	collist = []
	for col in info:
		for c in col:
			collist.append(c);

	print info
	return render_template('listkeyspaces.html',keys = collist);




@app.route("/cexplore/listcolfam")
def listcolumnfamilies():
	global keysp, column_family
	#if request.args['q']:
	#	keysp = request.args['q'];
	session = cluster.connect()
	query = "select columnfamily_name from system.schema_columnfamilies where keyspace_name=\'" + keysp + "\' ;"
	print query
	info = session.execute(query)
	collist = []
	for col in info:
		for c in col:
			collist.append(c);

	print info
	return render_template('listcolfam.html',cols = collist,keyspace= keysp.upper());



@app.route("/cexplore/listcol")
def listcolumn():
	global keysp, column_family
	column_family = request.args['q'];
	session = cluster.connect()
	query = "select column_name from system.schema_columns where keyspace_name=\'" + keysp + "\' AND columnfamily_name = \'" + column_family + "\';"
	print query
	info = session.execute(query)
	collist = []
	for col in info:
		for c in col:
			collist.append(c.upper())

	print info
	return render_template('listcolumns.html',cols = collist,colfam = column_family.upper());

@app.route("/cexplore/error")
def error_func():
    return render_template('error.html')


if __name__ == "__main__":
    app.run(debug=True)