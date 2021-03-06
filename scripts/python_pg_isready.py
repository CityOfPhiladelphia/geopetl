#!/usr/bin/env python3
import click
import psycopg2
import sys


@click.command()
@click.option('--host', required=True)
@click.option('--user', required=True)
@click.option('--dbname', required=True)
@click.option('--password', required=True)
def main(host,user,dbname,password):
	try:
	    conn = psycopg2.connect(user=user,
                                host=host,
                                password=password,
                                database=dbname,
                                connect_timeout=2)
	    sys.exit(0)
	except Exception as e:
	    error_message = str(e)
	    sys.exit(1)

main()
