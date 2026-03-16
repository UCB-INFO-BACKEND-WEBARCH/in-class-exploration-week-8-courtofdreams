#!/bin/bash
# Week 8 In-Class Exploration - Starter Test Commands

BASE_URL="http://localhost:8001"

echo "=== Notification Service (Synchronous) Test ==="
echo ""

# 1. Check API
echo "1. Check API status:"
curl -s $BASE_URL | python3 -m json.tool
echo ""

# 2. Send a notification - notice how SLOW this is!
echo "2. Send a notification (notice the 3+ second delay!):"
echo "   Timing the request..."
time curl -s -X POST $BASE_URL/notifications \
  -H "Content-Type: application/json" \
  -d '{"email": "student@berkeley.edu", "message": "Your assignment has been graded!"}'
echo ""

# 3. List notifications
echo "3. List all notifications:"
curl -s $BASE_URL/notifications | python3 -m json.tool
echo ""

# 4. Try sending multiple - each one blocks!
echo "4. Try sending 3 notifications (this will take 9+ seconds!):"
echo "   Starting..."
time (
  curl -s -X POST $BASE_URL/notifications \
    -H "Content-Type: application/json" \
    -d '{"email": "user1@example.com", "message": "Message 1"}' > /dev/null
  curl -s -X POST $BASE_URL/notifications \
    -H "Content-Type: application/json" \
    -d '{"email": "user2@example.com", "message": "Message 2"}' > /dev/null
  curl -s -X POST $BASE_URL/notifications \
    -H "Content-Type: application/json" \
    -d '{"email": "user3@example.com", "message": "Message 3"}' > /dev/null
)
echo ""
echo "See the problem? Each request blocks for 3 seconds!"
echo "Your task: Make POST return instantly using rq!"
