# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 21:56:39 2018

@author: hp
"""

#http://bionics.it/posts/luigi-tutorial
import luigi

class HelloWorld(luigi.Task):

    def requires(self):
        return None

    def output(self):
        return luigi.LocalTarget("data/HelloWorld.txt")

    def run(self):
        with self.output().open('w') as out_file:
            out_file.write('This is HelloLuigi')


class HelloLuigi(luigi.Task):

    def requires(self):
        return HelloWorld()

    def output(self):
        return luigi.LocalTarget("data/HelloLuigi.txt")

    def run(self):
        with self.output().open('w') as out_file:
            for line in self.input().open('r'):
                out_file.write(line)

if __name__ == '__main__':
    luigi.run()
