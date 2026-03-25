import sys

def generar_compose(output_file, cant_clientes):
    compose = f"""name: tp0
services:
  server:
    container_name: server
    image: server:latest
    entrypoint: python3 /main.py
    environment:
      - PYTHONUNBUFFERED=1
      - LOGGING_LEVEL=DEBUG
    networks:
      - testing_net
"""
    
    for i in range(1, cant_clientes + 1):
        client_block = f"""
  client{i}:
    container_name: client{i}
    image: client:latest
    entrypoint: /client
    environment:
      - CLI_ID={i}
      - CLI_LOG_LEVEL=DEBUG
    networks:
      - testing_net
    depends_on:
      - server
"""
        compose += client_block

    network = """
networks:
  testing_net:
    ipam:
      driver: default
      config:
        - subnet: 172.25.125.0/24
"""
    compose += network


    with open(output_file, 'w') as f:
        f.write(compose)

if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(1)
    
    output_file = sys.argv[1]
    cant_clientes = int(sys.argv[2])
    generar_compose(output_file, cant_clientes)