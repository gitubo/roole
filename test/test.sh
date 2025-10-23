pkill -9 router worker

./router 1 6000 6001 6002 &
sleep 1

./worker 100 5000 5001 1 127.0.0.1 6000 6001 &
sleep 1

./client 127.0.0.1 6002 "Hello World"