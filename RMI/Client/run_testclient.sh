# Usage: ./run_testclient.sh [<server_hostname> [<server_rmiobject>]]
echo "Performing a clean compile..."
make clean && make test-client
echo "Executing the TestClient..."
java -Djava.security.policy=java.policy -cp ../Server/RMIInterface.jar:. TestClient.RMITestClient $1 $2