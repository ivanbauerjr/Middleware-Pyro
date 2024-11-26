Passos para inicialização do sistema:

Inicie o servidor de nomes:
python -m Pyro5.nameserver

Inicie o lider:
python -m lider

Inicie o Cluster de Brokers:
python -m broker
ou, com argumentos:
python -m broker Broker_1 Votante
python -m broker Broker_2 Votante
python -m broker Broker_3 Observador


Inicie o consumidor e publicador:
python -m consumidor
python -m publicador