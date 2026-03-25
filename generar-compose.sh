#!/bin/bash

if [ "$#" -ne 2 ]; then
    echo "Formato: $0 <archivo_salida> <cantidad_clientes>"
    exit 1
fi

output_file="$1"
cant_clientes="$2"

echo "Archivo salida: $output_file"
echo "Cantidad clientes: $cant_clientes"

python3 generar-compose.py "$output_file" "$cant_clientes"