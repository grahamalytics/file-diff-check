import sys
import difflib
import pandas as pd
import argparse
from contextlib import redirect_stdout

def main():
    parser = argparse.ArgumentParser(description='Check for differences between data warehouses')

    parser.add_argument("--fileType", "-f", required=True,
                        choices=['.csv', '.tsv', '.CSV', '.TSV', 'csv', 'tsv', 'CSV', 'TSV'],
                        help="Type of files being compared")

    print('You are about to run an analysis to identify differences between data files LOD and CDW.\n')
    print('File types currently supported include .CSV and .TSV\n')
    print('This module is designed to read files containing 3 fields (in order): \n\tTRXN DATE \n\tTRXN HEADER ID \n\tMEMBER ID\n')
    print('The column headers do not need to adhere to any naming conventions, however must be ordered as listed above.\n')

    args = parser.parse_args()

    if args.fileType in ['.csv', '.CSV', 'csv', 'CSV']:
        sep = ','
    elif args.fileType in ['.tsv', '.TSV', 'tsv', 'TSV']:
        sep = '\t'
    else:
        print('File type provided currently not supported. Please reach out to script owner. Exiting program')
        exit()


    lodFile = input('>>>> Enter file name or path to LOD file for comparison to CDW:')
    cdwFile = input('>>>> Enter file name or path to CDW file for comparison to LOD:')
    desc = input('>>>> Enter descriptor to be appended to file names for identification:')

    # read in files into pandas dataframe for analysis
    lod = pd.read_csv(lodFile,
                      sep=sep,
                      skiprows=1,
                      names=['TXNDATE', 'TXNHEADERID', 'LOYALTYIDNUMBER'],
                      dtype={'TXNDATE': 'str',
                              'TXNHEADERID': 'int64',
                              'LOYALTYIDNUMBER': 'int64'})

    cdw = pd.read_csv(cdwFile,
                      sep=sep,
                      skiprows=1,
                      names=['TXNDATE', 'TXNHEADERID', 'LOYALTYIDNUMBER'],
                      dtype={'TXNDATE': 'str',
                            'TXNHEADERID': 'int64',
                            'LOYALTYIDNUMBER': 'int64'})

    ## convert 'TXNDATE' field to date
    lod['TXNDATE'] = pd.to_datetime(lod['TXNDATE'], format='%m/%d/%Y')
    cdw['TXNDATE'] = pd.to_datetime(cdw['TXNDATE'], format='%m/%d/%Y')


    # create dictionaries for containing results
    lodStats = {}
    cdwStats = {}

    ## total distinct trxn
    lodStats['totalTrxn'] = lod.loc[:, 'TXNHEADERID'].nunique()
    cdwStats['totalTrxn'] = cdw.loc[:, 'TXNHEADERID'].nunique()

    ## total distinct members
    lodStats['totalMembers'] = lod.loc[:, 'LOYALTYIDNUMBER'].nunique()
    cdwStats['totalMembers'] = cdw.loc[:, 'LOYALTYIDNUMBER'].nunique()

    ## total trxn and members per day
    lodTrxnByDay = lod.groupby('TXNDATE')['TXNHEADERID'].nunique().reset_index().sort_values(by='TXNDATE')
    lodMembersByDay = lod.groupby('TXNDATE')['LOYALTYIDNUMBER'].nunique().reset_index().sort_values(by='TXNDATE')
    cdwTrxnByDay = cdw.groupby('TXNDATE')['TXNHEADERID'].nunique().reset_index().sort_values(by='TXNDATE')
    cdwMembersByDay = cdw.groupby('TXNDATE')['LOYALTYIDNUMBER'].nunique().reset_index().sort_values(by='TXNDATE')

    ## total trxn by members
    lodMemberTrxn = lod.groupby('LOYALTYIDNUMBER')['TXNHEADERID'].nunique().reset_index().sort_values(by='LOYALTYIDNUMBER')
    cdwMemberTrxn = cdw.groupby('LOYALTYIDNUMBER')['TXNHEADERID'].nunique().reset_index().sort_values(by='LOYALTYIDNUMBER')

    # write meta-data stats to file before performing document diff
    lodTrxnByDay.to_csv('lodTrxnByDay-{desc}.csv'.format(desc=desc), index=False)
    cdwTrxnByDay.to_csv('cdwTrxnByDay-{desc}.csv'.format(desc=desc), index=False)
    lodMembersByDay.to_csv('lodMembersByDay-{desc}.csv'.format(desc=desc), index=False)
    cdwMembersByDay.to_csv('cdwMembersByDay-{desc}.csv'.format(desc=desc), index=False)
    lodMemberTrxn.to_csv('lodMemberTrxn-{desc}.csv'.format(desc=desc), index=False)
    cdwMemberTrxn.to_csv('cdwMemberTrxn-{desc}.csv'.format(desc=desc), index=False)

    # cleanup
    del(cdw, lod, cdwStats, lodStats, lodTrxnByDay, cdwTrxnByDay, lodMembersByDay, cdwMembersByDay, lodMemberTrxn, cdwMemberTrxn)


    # document diff on trxnByDay
    with open('lodTrxnByDay-{desc}.csv'.format(desc=desc), 'r') as lodTrxnByDay:
        with open('cdwTrxnByDay-{desc}.csv'.format(desc=desc)) as cdwTrxnByDay:
            diff = difflib.unified_diff(
                lodTrxnByDay.readlines(),
                cdwTrxnByDay.readlines(),
                fromfile='lodTrxnByDay-{desc}'.format(desc=desc),
                tofile='cdwTrxnByDay-{desc}'.format(desc=desc)
            )

            with open('trxnByDay-diff-output-{desc}.txt'.format(desc=desc), 'w') as output:
                with redirect_stdout(output):
                    for line in diff:
                        if line.split()[0][0] in ['+', '-', '@']:
                            sys.stdout.write(line)

    # cleanup
    del(lodTrxnByDay, cdwTrxnByDay, output)


    # document diff on membersByDay
    with open('lodMembersByDay-{desc}.csv'.format(desc=desc), 'r') as lodMembersByDay:
        with open('cdwMembersByDay-{desc}.csv'.format(desc=desc)) as cdwMembersByDay:
            diff = difflib.unified_diff(
                lodMembersByDay.readlines(),
                cdwMembersByDay.readlines(),
                fromfile = 'lodMembersByDay-{desc}'.format(desc=desc),
                tofile = 'cdwMembersByDay-{desc}'.format(desc=desc)
            )

            with open('membersByDay-diff-output-{desc}.txt'.format(desc=desc), 'w') as output:
                with redirect_stdout(output):
                    for line in diff:
                        if line.split()[0][0] in ['+', '-', '@']:
                            sys.stdout.write(line)

    # cleanup
    del(lodMembersByDay, cdwMembersByDay, output)


    # document diff on memberTrxn
    with open('lodMemberTrxn-{desc}.csv'.format(desc=desc), 'r') as lodMemberTrxn:
        with open('cdwMemberTrxn-{desc}.csv'.format(desc=desc)) as cdwMemberTrxn:
            diff = difflib.unified_diff(
                lodMemberTrxn.readlines(),
                cdwMemberTrxn.readlines(),
                fromfile = 'lodMemberTrxn-{desc}'.format(desc=desc),
                tofile = 'cdwMemberTrxn-{desc}'.format(desc=desc)
            )

            with open('memberTrxn-diff-output-{desc}.txt'.format(desc=desc), 'w') as output:
                with redirect_stdout(output):
                    for line in diff:
                        if line.split()[0][0] in ['+', '-', '@']:
                            sys.stdout.write(line)

    # cleanup
    del(lodMemberTrxn, cdwMemberTrxn, output)


    # document diff on raw data
    with open(lodFile, 'r') as file1:
        with open(cdwFile, 'r') as file2:
            diff = difflib.unified_diff(
                file1.readlines(),
                file2.readlines(),
                fromfile= lodFile + '-' + desc,
                tofile= cdwFile +'-' + desc
            )

            with open('diff-output-{desc}.txt'.format(desc=desc), 'w') as output:
                with redirect_stdout(output):
                    for line in diff:
                        if line.split()[0][0] in ['+','-', '@']:
                            # print(line)
                            sys.stdout.write(line)

if __name__ == '__main__':
    main()
