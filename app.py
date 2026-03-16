"""
Notification Service API - Starter (Synchronous)

This version sends notifications SYNCHRONOUSLY.
Each request blocks for 3 seconds while "sending" the notification.

YOUR TASK: Convert this to use rq for background processing!
"""

import os
from rq import Queue
from rq.job import Job

from flask import Flask, jsonify, request
import time
import uuid
from datetime import datetime
from redis import Redis
from tasks import send_notification

app = Flask(__name__)

# In-memory store for notifications
notifications = {}

redis_conn = Redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
q = Queue("notifications", connection=redis_conn)
# job = q.enqueue("notifications")


def send_notification_sync(notification_id, email, message):
    """
    Send a notification (SLOW - blocks for 3 seconds!)

    In production, this would call an email service like Mailgun.
    We simulate the slow API with time.sleep().
    """
    print(f"[Sending] Notification {notification_id} to {email}...")

    # This is the problem - blocking for 3 seconds!
    time.sleep(3)

    sent_at = datetime.utcnow().isoformat() + "Z"
    print(f"[Sent] Notification {notification_id} at {sent_at}")

    return {
        "notification_id": notification_id,
        "email": email,
        "status": "sent",
        "sent_at": sent_at
    }


@app.route('/')
def index():
    return jsonify({
        "service": "Notification Service (Synchronous - SLOW!)",
        "endpoints": {
            "POST /notifications": "Send a notification (takes 3 seconds!)",
            "GET /notifications": "List all notifications",
            "GET /notifications/<id>": "Get a notification"
        }
    })


@app.route('/notifications', methods=['POST'])
def create_notification():
    """
    Send a notification.

    WARNING: This blocks for 3 seconds!
    The user has to wait while we "send" the notification.

    TODO: Convert this to use rq for background processing!
    """
    
    data = request.get_json()

    if not data or 'email' not in data:
        return jsonify({"error": "Email is required"}), 400

    # Create notification record
    notification_id = str(uuid.uuid4())
    email = data['email']
    message = data.get('message', 'You have a new notification!')
    
    # job = q.enqueue(send_notification, notification_id, email, message)
    # result = send_notification_sync(notification_id, email, message)
    # send_notification.delay(notification_id, email, message)
    result = send_notification.delay(
       notification_id, email, message
    )
    
    notification = {
        "id": notification_id,
        "email": email,
        "message": message,
        "sent_at": result.result.get("sent_at") if result.is_finished else None,
        "status": result.get_status(),
        "job_id": result.id
        
    }
    
    notifications[notification_id] = notification

    return jsonify({"job_id": result.id}), 202


@app.route('/notifications', methods=['GET'])
def list_notifications():
    """List all notifications."""
    return jsonify({
        "notifications": list(notifications.values())
    })


@app.route('/notifications/<notification_id>', methods=['GET'])
def get_notification(notification_id):
    """Get a single notification."""
    notification = notifications.get(notification_id)
    if not notification:
        return jsonify({"error": "Notification not found"}), 404
    return jsonify(notification)

@app.route('/jobs/<job_id>', methods=['GET'])
def get_job_status(job_id):
    """Get the status of a background job."""
    
    try:
        job = Job.fetch(job_id, connection=redis_conn)
    except Exception:
        return {"error": "Job not found"}, 404

    response = {
        "job_id": job_id,
        "status": job.get_status()
    }

    if job.is_finished:
        response["result"] = job.result
    elif job.is_failed:
        response["error"] = str(job.exc_info)

    return response


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=8001, debug=True)
