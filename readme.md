# Sistema Distribuido

Implementação de sistema distrobuido com os seguintes resuisitos:

- Monitora recursos.

- Usa gRPC e Sockets.

- Detecta falhas com Heartbeat.

- Elege um novo líder com o algoritmo Bully.

- Sincroniza o estado com Lamport e Snapshot.

- Envia dados para múltiplos clientes via Multicast.

- Controla o acesso com um token (chave de criptografia).

Para a execução do código, com o docker ou docker desktop
instalado, rode o seguinte comando:

```shell
docker-compose up --build
```

Para a finalização dos containers, execute o segiunte comando:

```shell
docker-compose down -v
```

Para a inicialização do client para o recebimento dos relatórios, 
primeito instale a biblioteca cryptography. O cliente iniciará 
idependendo dos containers docker

```shell
pip install cryptography
```

Com a biblioteca instalada execute o seguinte comando
para a inicialização do client:

```shell
python client.py
```