import argparse


def copyenv_parser():
    parser = argparse.ArgumentParser(
        prog = "copy_env", 
        description = "Copies DOTENV file", 
        formatter_class = argparse.RawTextHelpFormatter)

    parser.add_argument("in_file",  type=str, 
            help="Archivo de lectura de variables de ambiente usadas.")
    parser.add_argument("--out_file", type=str, required=False,
            help="Archivo de escritura de variables requeridas.")
    return parser

