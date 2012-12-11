import os, sys, io
import logging
import time
from datetime import datetime
import ConfigParser
import urllib2

from SCons.Script import Execute, Mkdir   # for Execute and Mkdir

def read_config(build_parser_file, default_parser):
    logger = logging.getLogger('scons-qserv')
    logger.debug("Reading build config file : %s" % build_parser_file)
    parser = ConfigParser.SafeConfigParser()
    parser.readfp(io.BytesIO(default_parser))
    parser.read(build_parser_file)

    logger.debug("Build configuration : ")
    for section in parser.sections():
       logger.debug("[%s]" % section)
       for option in parser.options(section):
        logger.debug("'%s' = '%s'" % (option, parser.get(section,option)))

    config = dict()
    config['base_dir']          = parser.get("qserv","base_dir")
    config['log_dir']           = parser.get("qserv","log_dir")
    config['geometry_src_dir']  = parser.get("qserv","geometry_src_dir")
    config['node_type']         = parser.get("qserv","node_type")
    config['mysqld_port']       = parser.get("mysqld","port")
    config['mysqld_pass']       = parser.get("mysqld","pass",raw=True)
    config['mysqld_data_dir']   = parser.get("mysqld","data_dir")
    config['mysqld_proxy_port'] = parser.get("mysql-proxy","port") 
    config['lsst_data_dir']     = parser.get("lsst","data_dir")
    config['cmsd_manager_port'] = parser.get("xrootd","cmsd_manager_port")
    config['xrootd_port']       = parser.get("xrootd","xrootd_port")

    config['dependencies']=dict()
    for option in parser.options("dependencies"):
        config['dependencies'][option] = parser.get("dependencies",option)

    return config 

def is_readable(dir):
    """
    Test is a dir is readable.
    Return a boolean
    """
    logger = logging.getLogger('scons-qserv')

    logger.debug("Checking read access for : %s", dir)
    logger = logging.getLogger('scons-qserv')
    try:
        os.listdir(dir)
        return True 
    except Exception as e:
        logger.debug("No read access to dir %s : %s" % (dir,e))
        return False

def is_writable(dir):
    """
    Test if a dir is writeable.
    Return a boolean
    """
    logger = logging.getLogger('scons-qserv')
    try:
        tmp_prefix = "write_tester";
        count = 0
        filename = os.path.join(dir, tmp_prefix)
        while(os.path.exists(filename)):
            filename = "{}.{}".format(os.path.join(dir, tmp_prefix),count)
            count = count + 1
        f = open(filename,"w")
        f.close()
        os.remove(filename)
        return True
    except Exception as e:
        logger.info("No write access to dir %s : %s" % (dir,e))
        return False


def exists_and_is_writable(dir) :
    """
    Test if a dir exists. If no creates it, if yes checks if it is writeable.
    Return a boolean
    """
    logger = logging.getLogger('scons-qserv')
    logger.debug("Checking existence and write access for : %s", dir)
    if not os.path.exists(dir):
	try:
            Execute(Mkdir(dir))
        except Exception as e:
            logger.info("Unable to create dir : %s : %s" % (dir,e))
            return False 
    elif not is_writable(dir):
        return False
    
    return True	 

def download_action(target, source, env):
    
    logger = logging.getLogger('scons-qserv')

    logger.debug("Target %s :" % target[0])
    logger.debug("Source %s :" % source[0])

    url = str(source[0])
    file_name = str(target[0])
    logger.debug("Opening %s :" % url)
    u = urllib2.urlopen(url)
    f = open(file_name, 'wb')
    meta = u.info()
    file_size = int(meta.getheaders("Content-Length")[0])
    logger.info("Downloading: %s Bytes: %s" % (file_name, file_size))

    file_size_dl = 0
    block_sz = 64 * 256 
    while True:
        buffer = u.read(block_sz)
        if not buffer:
            break

        file_size_dl += len(buffer)
        f.write(buffer)
        status = r"%10d  [%3.2f%%]" % (file_size_dl, file_size_dl * 100. / file_size)
        status = status + chr(8)*(len(status)+1)
        print(status)

    f.close()


def build_cmd_with_opts_action(config, target='install'):

    logger = logging.getLogger('scons-qserv')

    install_opts="--install-dir=\"%s\"" % config['base_dir']
    install_opts="%s --log-dir=\"%s\"" % (install_opts, config['log_dir'])
    install_opts="%s --mysql-data-dir=\"%s\"" % (install_opts, config['mysqld_data_dir'])
    install_opts="%s --mysql-port=%s" % (install_opts, config['mysqld_port'])
    install_opts="%s --mysql-proxy-port=%s" % (install_opts,config['mysqld_proxy_port'])
    install_opts="%s --mysql-pass=\"%s\"" % (install_opts,config['mysqld_pass'])
    install_opts="%s --cmsd-manager-port=%s" % (install_opts,config['cmsd_manager_port'])
    install_opts="%s --xrootd-port=%s" % (install_opts,config['xrootd_port'])
    
    if len(config['geometry_src_dir'])!=0 :
        install_opts="%s --geometry-dir=\"%s\"" % (install_opts,config['geometry_src_dir'])
    
    if config['node_type']=='mono' :
        install_opts="%s --mono-node" % install_opts
    elif config['node_type']=='master' :
        None
    elif config['node_type']=='worker' :
        None

    log_file_prefix = config['log_dir']
    if target=='qserv-only' :
        install_opts="%s --qserv" % install_opts
        log_file_prefix += "/QSERV-ONLY"
    elif target == 'clean-all' :
        install_opts="%s --clean-all" % install_opts
        log_file_prefix = "~/QSERV-CLEAN"
    elif target == 'init-mysql-db' :
        install_opts="%s --init-mysql-db" % install_opts
        log_file_prefix += "/QSERV-INIT-MYSQL-DB"
    else :
        log_file_prefix += "/INSTALL"
    log_file_name = log_file_prefix + "-" + datetime.now().strftime("%Y-%m-%d-%H:%M:%S") + ".log" 

    command_str = config['src_dir' ] + "/admin/qserv-install " 
    command_str += install_opts + " &> " + log_file_name
    
    #logger.debug("Launching perl install script with next command : %s" % command_str)
    return command_str
