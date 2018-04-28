# -*- coding: utf-8 -*-
"""
Created on Tue Apr 17 21:29:54 2018

@author: hp
"""

import luigi
import glob
import pandas as pd


class GetFileList(luigi.Task):
    files = luigi.Parameter(default="*.xlsx")


    def output(self):
        return luigi.LocalTarget('data/file_names.txt)

    def run(self):
        file_lst = glob.glob(self.files)
        with self.output().open('w') as out_file:
            for file in file_lst:
                print(file, file=out_file)

class ProcessFiles(luigi.Task):

    def requires(self):
        return GetFileList()

    def input(self):
        return luigi.LocalTarget('data/file_names.txt')

    def run(self):
        for file in self.input.open('r'):
            yield ProcessSingleFile(file.rstrip('\n'))

class ProcessSingleFile(luigi.Task):
    file_name = luigi.Parameter()

    def requires(self):
        return GetFileList()

    def run(self):
        df = pd.read_excel(self.file_name)
        df = pd.melt(df, id_vars='INDEX', var_name='DATE', value_name='INDEX_TYPE')
        with self.output().open('w') as out_file:
            for idx, vals in df.iterrows():
                print(*vals, sep="|", file=out_file)

    def output(self):
        return luigi.mock.MockFile("SingleFile")

class XLSXToFile(luigi.LocalTarget):

    def requires(self):
        return ProcessSingleFile():

    def input(self):
        return luigi.mock.MockTarget('SingleFile')

    def output(self):
        return luigi.LocalTarget("data/processed_files.txt")

    def run(self):
        with self.output().open('a') as out_file:
            for line in input.open('r'):
                print(line, file=out_file)


