import os
import unittest
import shutil
import pandas as pd
import numpy as np
from script import aggregate_files
from generate import generate_csv


class TestSomeModel(unittest.TestCase):

    def test_summary_table(self):
        """
        :return: проверка на то, что суммарное количество значений в итоговой таблице
        такое же, как было введено пользователем
        """
        os.mkdir('input_test')
        os.mkdir('output_test')
        generate_csv('input_test', '2002-01-01', 10, 2, 27)
        actual_date = '2002-01-08'
        aggregate_files(actual_date, 'input_test', 'output_test')
        aggregate_logs = pd.read_csv(f"output_test/{actual_date}.csv").drop("email", axis=1)
        self.assertEqual(189, aggregate_logs.sum().sum())
        shutil.rmtree('input_test')
        shutil.rmtree('output_test')

        os.mkdir('input_test')
        os.mkdir('output_test')
        generate_csv('input_test', '2002-01-01', 5, 2, 30)
        actual_date = '2002-01-08'
        aggregate_files(actual_date, 'input_test', 'output_test')
        aggregate_logs = pd.read_csv(f"output_test/{actual_date}.csv").drop("email", axis=1)
        self.assertEqual(150, aggregate_logs.sum().sum())
        shutil.rmtree('input_test')
        shutil.rmtree('output_test')

        os.mkdir('input_test')
        os.mkdir('output_test')
        generate_csv('input_test', '2002-01-01', 7, 2, 100)
        actual_date = '2002-01-08'
        aggregate_files(actual_date, 'input_test', 'output_test')
        aggregate_logs = pd.read_csv(f"output_test/{actual_date}.csv").drop("email", axis=1)
        self.assertEqual(700, aggregate_logs.sum().sum())
        shutil.rmtree('input_test')
        shutil.rmtree('output_test')

    def test_check_aggregate(self):
        """
        :return: проверка на то, с верными ли значениями создается таблица
        """
        os.mkdir('input_test')
        os.mkdir('output_test')
        actual_date = '2003-01-08'
        aggregate_files(actual_date, 'input_test', 'output_test')
        aggregate_logs = pd.read_csv(f"output_test/{actual_date}.csv").drop("email", axis=1)
        self.assertEqual(0, aggregate_logs.sum().sum())
        shutil.rmtree('input_test')
        shutil.rmtree('output_test')

        os.mkdir('input_test')
        os.mkdir('output_test')
        (pd.DataFrame([["test1@mail.com", "CREATE", '2022-01-01'],
                       ["test1@mail.com", "CREATE", '2022-01-01'],
                       ["test1@mail.com", "CREATE", '2022-01-01'],
                       ["test1@mail.com", "UPDATE", '2022-01-01'],
                       ["test2@mail.com", "READ", '2022-01-01'],
                       ["test2@mail.com", "UPDATE", '2022-01-01'],
                       ["test2@mail.com", "DELETE", '2022-01-01'],
                       ["test2@mail.com", "CREATE", '2022-01-01']])
         .to_csv('input_test/2022-01-01.csv', header=None, index=False))
        actual_date = '2022-01-07'
        aggregate_files(actual_date, 'input_test', 'output_test')
        aggregate_logs = pd.read_csv(f"output_test/{actual_date}.csv").drop("email", axis=1)

        print(type(aggregate_logs.values))
        self.assertEqual((np.array([[3, 0, 1, 0],
                                    [1, 1, 1, 1]]) - aggregate_logs.values).sum(), 0)

        self.assertEqual(8, aggregate_logs.sum().sum())
        shutil.rmtree('input_test')
        shutil.rmtree('output_test')

        os.mkdir('input_test')
        os.mkdir('output_test')
        (pd.DataFrame([["test1@mail.com", "READ", '2022-01-01'],
                       ["test1@mail.com", "CREATE", '2022-01-01'],
                       ["test1@mail.com", "CREATE", '2022-01-01'],
                       ["test1@mail.com", "UPDATE", '2022-01-01'],
                       ["test2@mail.com", "CREATE", '2022-01-01'],
                       ["test2@mail.com", "CREATE", '2022-01-01'],
                       ["test2@mail.com", "DELETE", '2022-01-01'],
                       ["test2@mail.com", "CREATE", '2022-01-01']])
         .to_csv('input_test/2022-01-01.csv', header=None, index=False))
        actual_date = '2022-01-07'
        aggregate_files(actual_date, 'input_test', 'output_test')
        aggregate_logs = pd.read_csv(f"output_test/{actual_date}.csv").drop("email", axis=1)

        print(type(aggregate_logs.values))
        self.assertEqual((np.array([[2, 1, 1, 0],
                                    [3, 0, 0, 1]]) - aggregate_logs.values).sum(), 0)

        self.assertEqual(8, aggregate_logs.sum().sum())
        shutil.rmtree('input_test')
        shutil.rmtree('output_test')
