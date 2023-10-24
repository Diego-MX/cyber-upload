# DX, Epic Bank
# CDMX, 17 octubre '23

from subprocess import check_call
from sys import argv
from pyspark.dbutils import DBUtils
from pyspark.sql import SparkSession
try: import yaml                    # pylint: disable=multiple-statements
except ImportError: yaml = None     # pylint: disable=multiple-statements


REQS_FILE = '../reqs_dbks.txt'
USER_FILE = '../user_databricks.yml'
EPIC_REF = 'gh-1.6' 
V_TYPING = '4.7.1'

# pylint: disable=redefined-outer-name
def install_epicpy(epic_ref, reqs, user_file, typing, verbose):
    # Orden argumentos ≠ orden programático
    if typing: 
        _install_typing(typing)
    if reqs or (yaml is None): 
        _install_reqs(reqs)
    _install_with_token(epic_ref, user_file, verbose)


def _install_with_token(gh_ref=None, user_file=None, verbose=False): 
    import yaml     
    gh_ref = gh_ref or EPIC_REF
    user_file = user_file or USER_FILE
    verbose = verbose or (gh_ref != EPIC_REF)
    spark = SparkSession.builder.getOrCreate()
    dbutils = DBUtils(spark)
    with open(user_file, 'r') as _f:        # pylint: disable=unspecified-encoding
        u_dbks = yaml.safe_load(_f)
    call_keys = {
        'url' : 'github.com/Bineo2/data-python-tools.git', 
        'ref' : gh_ref, 
        'token' : dbutils.secrets.get(u_dbks['dbks_scope'], u_dbks['dbks_token'])}
    _pip_install("git+https://{token}@{url}@{ref}".format(**call_keys))
    if verbose: 
        import epic_py
        info = {'Epic Ref': gh_ref, 'Epic Ver': epic_py.__version__}
        print(info)
    return 

def _install_reqs(reqs=None): 
    reqs = reqs if isinstance(reqs, str) else REQS_FILE
    _pip_install('-r', reqs)
    return 

def _install_typing(typing=None): 
    typing = typing if isinstance(typing, str) else V_TYPING
    _pip_install('--upgrade', f"typing-extensions=={typing}")
    return 

def _pip_install(*args): 
    check_call(['pip', 'install', *args])
    return


if __name__ == '__main__': 
    epic_ref  = argv[1] if len(argv) > 1 else EPIC_REF
    reqs_file = argv[2] if len(argv) > 2 else REQS_FILE
    user_file = argv[3] if len(argv) > 3 else USER_FILE
    v_typing  = argv[4] if len(argv) > 4 else V_TYPING
    
    install_epicpy(epic_ref, reqs_file, user_file, v_typing)
