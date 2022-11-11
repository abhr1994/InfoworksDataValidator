import click

@click.command()
@click.option("--name",required=True,help="Enter the name of the user")
def welcome(name):
    click.echo(f"Welcome to Infoworks Data Validator Tool! {name}")

if __name__ == '__main__':
    welcome()