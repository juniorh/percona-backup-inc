#!/usr/bin/env python
###################
# Usage
# python pyinnobackup.py --help
# python pyinnobackup.py --backup --dir /backup/test/1 --user root --password 123456 --force
# python pyinnobackup.py --restore --dir /backup/test/1 
# Directory tree
#   pathDir/info.out
#   pathDir/trace.out
#     full_{timestamp:YYYY-MM-DD_hh-ss} from_lsn to_msn
#     inc_1_{timestamp:YYYY-MM-DD_hh-ss} from_lsn to_msn
#     inc_2_{timestamp:YYYY-MM-DD_hh-ss} from_lsn to_msn
#   pathDir/.log
#   pathDir/full_{timestamp:YYYY-MM-DD_hh-ss}
#   pathDir/inc_1_{timestamp:YYYY-MM-DD_hh-ss}
#   pathDir/inc_2_{timestamp:YYYY-MM-DD_hh-ss}

from datetime import datetime
import subprocess
import argparse
import logging
import random
import sys
import os

curTime = datetime.now()
workDir = None,
tmpDir = '/tmp/pyinnobackup_'+str(random.randrange(1000000,9999999))
backupDir = None
args = None
logger = None
logFile = None
isFullBackup = True
listBackupPath = []
lastBackupDir = None
cmd = None

def init_log():
  global logger
  global logFile
  global args
  logFormat = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
  logger = logging.getLogger('pyinnbac')
  logger.setLevel(logging.DEBUG)
  handlerStd = logging.StreamHandler()
  handlerStd.setFormatter(logFormat)
  handlerStd.setLevel(100)
  logger.addHandler(handlerStd)
  if args.debug:
    #Log Stream
    handlerStd.setLevel(logging.DEBUG)
  if args.force:
    logger.debug('Force argument is enable')
    if not os.path.exists(args.dir):
      try:
        os.makedirs(args.dir)
      except Exception, err:
        logger.debug('Cant create directory %s - Error: %s', workDir, err)        
        sys.exit()
  #Log File
  if args.log:
    logFile = args.log
  else:
    logFile = args.dir+'/debug.log'
  logger.debug('Set log file %s', logFile)
  handler = logging.FileHandler(logFile)
  handler.setFormatter(logFormat)
  logger.addHandler(handler)

def get_args_parser():
  parser = argparse.ArgumentParser(add_help=False)
  parser.add_argument(
    "-u", "--user",
    default='root',
    nargs='?',
    type=str,
    help="User for login")
  parser.add_argument(
    "-p", "--password",
    default='',
    nargs='?',
    type=str,
    help="User for login")
  parser.add_argument(
    "-h", "--host",
    default=False,
    nargs='?',
    type=str,
    help="Host database server")
  parser.add_argument(
    "-P", "--port",
    default='3306',
    nargs='?',
    type=str,
    help="Port database server")
  parser.add_argument(
    "-c", "--config",
    default=False,
    nargs='?',
    type=str,
    help="MySQL Config [MY.CNF]")
  parser.add_argument(
    "-d", "--dir",
    default=False,
    nargs='?',
    type=str,
    help="Directory for backup data")
  parser.add_argument(
    "-f", "--force",
    default=False,
    action='store_true',
    help="Force create new backup directory if not exist")
  parser.add_argument(
    "-l", "--log",
    default=False,
    nargs='?',
    type=str,
    help="Select variable")
  parser.add_argument(
    "--daily",
    default=False,
    action='store_true',
    help="Change backup directory every day")
  parser.add_argument(
    "-b", "--backup",
    default=False,
    action='store_true',
    help="Backup action")
  parser.add_argument(
    "-r", "--restore",
    default=False,
    action='store_true',
    help="Restore action")
  parser.add_argument(
    "-v", "--debug",
    default=False,
    action='store_true',
    help="Print progress")
  parser.add_argument(
    "--check",
    default=False, 
    action='store_true',
    help="Check working directory directory")
  parser.add_argument(
    "--help",
    default=False,
    action='store_true',
    help="Show this help"
  )
  return parser

def check_args():
  global workDir
  global args
  if args.help:
    sys.exit(parser.print_help())
  if args.backup and not args.restore:
    #Backup action
    pass
  if args.restore and not args.backup:
    #Restore action
    pass
  if (args.restore and args.backup) or (not args.restore and not args.backup) :
    sys.exit(parser.print_help())
  if args.dir:
    logger.debug('ARGS: %s',args)
    if args.daily:
      logger.debug('Enable daily backup directory')
      workDir = args.dir+'/sql_backup_'+curTime.strftime('%Y%m%d')
      if not os.path.exists(workDir):
        logger.debug('Daily backup directory %s not exist', workDir)
        try:
          os.makedirs(workDir)
        except Exception, err:
          logger.debug('Cant create directory %s - Error: %s', workDir, err)        
          sys.exit()
    else:
      workDir = args.dir
    logger.debug('Set working directory: %s', workDir)
    logger.debug('Set tmp directory: %s', tmpDir)
  else:
    logger.debug('--dir option not set') 
    sys.exit(parser.print_help())
  return 0

def match_xtrabackup_checkpoints(tDir,lsnFrom,lsnTo):
  try:
    fCP = open(tDir+'/xtrabackup_checkpoints','r')
  except Exception, err:
    logger.debug('Error read info.out: %s', err)        
    sys.exit()
  lineCP = fCP.read().split('\n')
  if lineCP[1].split(' = ')[1] == lsnFrom: 
    if lineCP[2].split(' = ')[1] == lsnTo: 
      return True
    else:
      logger.debug('Xtrabackup_checkpoints %s not match with trace.out to_lsn = %s', lineCP[2], lsnTo)
      return False
  else:
    logger.debug('Xtrabackup_checkpoints %s not match with trace.out from_lsn = %s', lineCP[1], lsnFrom)
    return False

def check_workdir():
  traceDir = None
  traceLsnFrom = None
  traceLsnTo = None
  fTrace = None
  fInfo = None 
  nLine = None
  global lastBackupDir
  global args
  logger.info("Check backup directory %s", workDir)
  #check workDir
  if not os.path.exists(workDir):
    logger.debug("Directory %s not exist", workDir)
    if args.force:
      logger.debug("Create new backup directory %s", workDir)
    else:
      logger.debug("No --force argument, stop process")
      sys.exit()
  #check trace.out
  try:
    os.path.exists(workDir+'/trace.out')
    os.path.isfile(workDir+'/trace.out')
    fTrace = open(workDir+'/trace.out')
  except Exception, err:
    logger.debug('Error read info.out: %s', err)        
    sys.exit()
  logger.info('Croscheck info.out trace.out and xtrabackup_checkpoints files')
  lineTrace = fTrace.readline()
  nLine = 0
  while lineTrace: 
    nLine = nLine + 1
    splitLineTrace = lineTrace.split('\n')[0].split()
    if len(splitLineTrace) == 3:
      logger.debug('Track on directory %s, from_lsn %s, to_lsn %s', splitLineTrace[0], splitLineTrace[1], splitLineTrace[2])
      traceDir = splitLineTrace[0]
      if args.restore:
        listBackupPath.append(traceDir)
      if traceDir.split('_')[0] == 'full':
        traceLsnFrom = splitLineTrace[1]
        traceLsnTo  = splitLineTrace[2]
      elif traceDir.split('_')[0] == 'inc':
        #check prevLsnTo == nextLsnFrom
        if traceLsnTo == splitLineTrace[1]:
          traceLsnFrom = splitLineTrace[1]
          traceLsnTo = splitLineTrace[2]
        else:
          logger.debug('Previous lsn_to %s not match with next lsn_from %s', traceLsnTo, splitLineTrace[1])
      if not match_xtrabackup_checkpoints(workDir+'/'+traceDir, traceLsnFrom, traceLsnTo):
        logger.debug('Checking LSN between trace.out and xtrabackup_checkpoints didnt match')
    else:
      logger.debug('Corrupt line file trace.out')
      sys.exit()
    lineTrace = fTrace.readline()
  logger.info('Finish tracking %s directory based on .track', nLine)
  #check info.out
  if os.path.exists(workDir+'/info.out'):
    if os.path.isfile(workDir+'/info.out'):
      try:
        fInfo = open(workDir+'/info.out')
        lFileInfo = fInfo.readline().split('\n')[0]
        fInfo.close()
      except Exception, err:
        logger.debug('Error read info.out: %s', err)
        sys.exit()
      logger.info('Croscheck info.out and last line on trace.out')
      logger.debug('.info = %s', lFileInfo)
      if lFileInfo.split()[0] == traceDir:
        if lFileInfo.split()[1] == traceLsnFrom:
          if lFileInfo.split()[2] == traceLsnTo:
            logger.debug('Croscheck info.out and trace.out match') 
            lastBackupDir = traceDir

def prepare_dir():
  global isFullBackup
  global args
  global lastBackupDir
  global backupDir
  #if not os.path.exists(tmpDir):
  #  try:
  #    os.makedirs(tmpDir)
  #  except Exception, err:
  #    logger.exception('Cant create tmp dir %s: %s', tmpDir, err)
  #    sys.exit()
  #else:
  #  logger.debug('tmp dir exist on %s', tmpDir)
  #check info.out & trace.out files
  if os.path.exists(workDir+'/info.out'):
    if os.path.isfile(workDir+'/info.out'):
      if os.path.exists(workDir+'/trace.out'):
        if os.path.isfile(workDir+'/trace.out'):
          check_workdir()
          isFullBackup = False
  #CreateDir
  if isFullBackup:
    logger.info('Process full backup')
    backupDir = 'full_0_'+curTime.strftime('%Y-%m-%d_%H-%M-%S')
  else:
    logger.info('Process increment backup')
    if len(lastBackupDir.split('_')) == 4:
      sLastBackupDir = lastBackupDir.split('_')
      if sLastBackupDir[0] == 'full':
        backupDir = 'inc_1_'+curTime.strftime('%Y-%m-%d_%H-%M-%S')
      if sLastBackupDir[0] == 'inc':
        numInc = int(sLastBackupDir[1])+1
        backupDir = 'inc_'+str(numInc)+'_'+curTime.strftime('%Y-%m-%d_%H-%M-%S')
  logger.debug('Set backup dir %s/%s',workDir,backupDir)
  
def exec_innobackupex_backup():
  global logFile
  proc = None
  cmd = ['innobackupex']
  if args.backup:
    cmd.append('--user='+args.user)
    cmd.append('--password='+args.password)
    if args.host:
      cmd.append('--host='+args.host)
      cmd.append('--port='+args.port)
    cmd.append('--no-timestamp')
    if not isFullBackup:
      cmd.append('--incremental')
      cmd.append('--incremental-basedir='+workDir+'/'+lastBackupDir)
    cmd.append(workDir+'/'+backupDir)
  logger.debug('Execute command: %s',cmd)
  with open(logFile,'a+') as outProc:
    proc = subprocess.Popen(cmd,stdout=outProc,stderr=subprocess.STDOUT)
    proc.communicate()
    if proc.returncode != 0:
      logger.debug('Error execute command innobackupex backup')
      sys.exit()
  logger.debug('Success executing command')

def update_info():
  checkpointsPath = workDir+'/'+backupDir+'/xtrabackup_checkpoints'
  checkpoints = None
  backup_type = None
  from_lsn = None
  to_lsn = None 
  try:
    checkpoints = open(checkpointsPath,'r').read().split('\n')
    backup_type = checkpoints[0].split(' = ')[1]
    from_lsn = checkpoints[1].split(' = ')[1]
    to_lsn = checkpoints[2].split(' = ')[1]
  except Exception, err:
    logger.exception('Cant open file checkpoints: %s\n%s', checkpointsPath, err)
    sys.exit() 
  if (isFullBackup and backup_type=='full-backuped') or (not isFullBackup and backup_type=='incremental'):
    logger.debug('Update file info.out and trace.out in %s', workDir)
    insLine = backupDir+' '+from_lsn+' '+to_lsn
    fInfo = open(workDir+'/info.out','w')
    fInfo.write(insLine)
    fInfo.close()
    fTrace = open(workDir+'/trace.out','a+')
    fTrace.write(insLine+'\n')
    fTrace.close()

def exec_innobackupex_restore():
  logger.debug(listBackupPath)
  for path in listBackupPath:
    cmd = ['innobackupex']
    cmd.append('--apply-log')
    if not path == listBackupPath[-1] :
      cmd.append('--redo-only')
    cmd.append(workDir+'/'+listBackupPath[0])
    if not path[0:4] == 'full':
      cmd.append('--incremental-dir='+workDir+'/'+path)
    logger.debug('Running restore command: %s',cmd) 
    with open(logFile,'a+') as outProc:
      proc = subprocess.Popen(cmd,stdout=outProc,stderr=subprocess.STDOUT)
      proc.communicate()
      if proc.returncode != 0:
        logger.debug('Error execute command innobackupex restore')
        sys.exit()
  logger.debug('Finish prepare backup with --apply-log and --redo-only')

def exec_copyback():
  cmd = ['innobackupex']
  cmd.append('--copy-back')
  cmd.append(workDir+'/'+listBackupPath[0])
  logger.debug('Running copy command: %s', cmd)
  with open(logFile) as outProc:
    proc = subprocess.Popen(cmd,stdout=outProc,stderr=subprocess.STDOUT)
    proc.communicate()
    if proc.returncode != 0:
      logger.debug('Error execute command innobackupex restore')
      sys.exit()
  logger.debug('Finish copy backup dir')

def main():
  global args 
  global tmpDir 
  parser = get_args_parser()
  args = parser.parse_args()
  init_log()
  check_args()
  tmpDir = workDir
  if args.check:
    check_workdir()
    logger.info('Finish checking')
  elif args.backup:
    #Prepare directory
    prepare_dir()
    #Do Backup Action
    logger.info("Start backup process")
    exec_innobackupex_backup()
    update_info()
  elif args.restore:
    logger.info("Start restore process")
    check_workdir()
    #Do Restore Action
    exec_innobackupex_restore()
    #exec_copyback()
 
if __name__ == '__main__':
  sys.exit(main())
