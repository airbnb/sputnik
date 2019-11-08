package com.airbnb.sputnik.hive

import com.airbnb.sputnik.Metrics

object DefaultInsert extends Insert(metrics = new Metrics)
