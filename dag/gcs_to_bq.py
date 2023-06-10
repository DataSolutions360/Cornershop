import airflow

from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import KubernetesPodOperator as AirflowKubernetesPodOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow import DAG
import jsonimport logging
from airflow.models import DAG, Variable, XCom
from airflow.operators.dummy import DummyOperator

from datetime import timedelta, datetime

import numpy as np
import pandas as pd
import os
class KubernetesPodOperator(AirflowKubernetesPodOperator):