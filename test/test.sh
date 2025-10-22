pkill -9 router worker

./router 1 5000 &
sleep 1

./worker 100 6000 4 127.0.0.1 5000 &
sleep 1

./client 127.0.0.1 5000 "Hello from Roole!"