# Usage: ./run_client.sh [<server_hostname> [<server_rmiobject>]]
echo "Performing a clean compile..."
make clean && make
echo "Executing the Client..."
java -Djava.security.policy=java.policy -cp ../Server/RMIInterface.jar:. Client.RMIClient $1 $2
