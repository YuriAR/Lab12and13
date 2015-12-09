import requests
import boto
import boto.sqs
import boto.sqs.queue
from boto.sqs.message import Message
from boto.sqs.connection import SQSConnection
from boto.exception import SQSError
import sys
import os
import json
from subprocess import Popen, PIPE
from flask import Flask, Response, render_template , request, redirect, url_for
from werkzeug import secure_filename
from tempfile import mkdtemp

app = Flask(__name__)

def get_conn():
	url = "http://ec2-52-30-7-5.eu-west-1.compute.amazonaws.com:81/key"
	data = requests.get(url).text
	keys = data.split(":")
	key_id = keys[0]	
	secret_access_key = keys[1]
	return boto.sqs.connect_to_region("eu-west-1",aws_access_key_id=key_id,aws_secret_access_key=secret_access_key)

@app.route("/", methods=["GET"])
def index():
	return Response(response=json.dumps(boto.Version), mimetype="application/json")

@app.route("/queues", methods=["GET"])
def list_queues():
	all = []
	conn = get_conn()
	rs = conn.get_all_queues()
	for q in rs:
		all.append(q.name)
	resp = json.dumps(all)
	return Response(response=resp,mimetype="application/json")	

@app.route("/queues", methods=["POST"])
def create_queue():
	conn = get_conn()
	body = request.get_json(force=True)
	name = body["name"]
	queue = conn.create_queue(name)
	resp = json.dumps("Queue created")
	return Response(response=resp,mimetype="application/json")

@app.route("/queues/<name>", methods=["DELETE"])
def delete_queue(name):
	conn = get_conn()
	queue = conn.get_queue(name)
	conn.delete_queue(queue)
	resp = json.dumps("Queue " + name + " deleted")
        return Response(response=resp,mimetype="application/json")

@app.route("/queues/<name>/msgs/count", methods=["GET"])
def number_of_messages(name):
	conn = get_conn()
	queue = conn.get_queue(name)
	messages = queue.get_messages()
	resp = json.dumps(str(len(messages)))
	return Response(response=resp,mimetype="application/json")

@app.route("/queues/<name>/msgs", methods=["POST"])
def write_message(name):
	conn = get_conn()
	m = Message()
	body = request.get_json(force=True)
	m.set_body(body['content'])
	queue = conn.get_queue(name)
	queue.write(m)
	resp = json.dumps("Message written")
        return Response(response=resp,mimetype="application/json")

@app.route("/queues/<name>/msgs", methods=["GET"])
def read_message(name):
	conn = get_conn()
	queue = conn.get_queue(name)
	messages = queue.get_messages()
	if len(messages) > 0:
		resp = json.dumps(messages[0].get_body())
	else:
		resp = json.dumps("No messages")
	return Response(response=resp,mimetype="application/json")

@app.route("/queues/<name>/msgs", methods=["DELETE"])
def consume_message(name):
	conn = get_conn()
	queue = conn.get_queue(name)
	messages = queue.get_messages()
	if len(messages) > 0:
		resp = json.dumps(messages[0].get_body())
		queue.delete_message(messages[0])
	else:
		resp = json.dumps("No messages")
	return Response(response=resp,mimetype="application/json")
	

if __name__ == "__main__":
    app.run(host="0.0.0.0")
