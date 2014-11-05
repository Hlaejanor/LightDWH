<?php

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

class ChartData extends LDWH {

    public $title;
    public $subtitle;
    public $xAxis;
    public $yAxis;
    public $tooltip;
    public $plotOptions;
    public $legend;
    public $credits;
    public $series;

    function __construct() {
        $this->title = new stdClass();
        $this->chart = new stdClass();
        $this->subtitle = new stdClass();
        $this->xAxis = new stdClass();
        $this->yAxis = new stdClass();
        $this->plotOptions = new stdClass();
        $this->tooltip = new stdClass();
        $this->xAxis->categories = new stdClass();
        $this->xAxis->title = new stdClass();
        $this->xAxis->title->text = null;
        $this->xAxis->labels = new stdClass();
        $this->legend = new stdClass();
        $this->yAxis->title = new stdClass();
        $this->yAxis->labels = new stdClass();
        $this->plotOptions->bar = new stdClass();
        $this->plotOptions->bar->dataLabels = new stdClass();
        $this->credits = new stdClass();
    }

}

class Chart {

    function __construct() {
        $this->data = new ChartData();

        $this->data->chart->type = 'bar';
        $this->data->title->text = 'Overview somthing things';
        $this->data->subtitle->text = 'Source';

        $this->data->xAxis->categories = array('Africa', 'America', 'Asia', 'Europe', 'Oceania');
        $this->data->xAxis->title->text = null;
        $this->data->yAxis->min = 0;
        $this->data->yAxis->title->text = 'Amount';
        $this->data->yAxis->labels->overflow = 'justify';
        $this->data->tooltip->valueSuffix = ' millions';
        $this->data->plotOptions->bar->dataLabels->enabled = true;
        $this->data->legend->layout = 'vertical';
        $this->data->legend->align = 'right';
        $this->data->legend->verticalAlign = 'top';
        $this->data->legend->x = -40;
        $this->data->legend->y = 100;
        $this->data->legend->floating = true;
        $this->data->legend->borderWidth = 1;
        $this->data->legend->backgroundColor = '#FFFFFF';
        $this->data->legend->shadow = true;
        $this->data->credits->enabled = false;
        $this->data->series = array();
    }

    function employ() {
        $this->data->series = array(
            array('name' => 'Year 1800', "data" => array(107, 31, 635, 203, 2))
            , array('name' => 'Year 1900', "data" => array(133, 156, 947, 408, 6))
            , array('name' => 'Year 2008', "data" => array(133, 156, 947, 408, 6))
        );
    }

    function printChart() {

        print "$('#container').highcharts(" . json_encode($this->data) . ");";
    }

    // This function will summarize data for the chart
    // It requires to know which variables to put on the series 'axis', what column to use as a
    // as the measurement variable and finally what variable should be on the Y Axis.

    function lookupData(
    $tableName, // The BL_tableName where the data is prepared
            $factVariable, // The fact table which are used to analyse the data
            $aggregationType, // The aggregationType, can be count, sum, avg, min, max or mod
            $seriesDim, // The dimension which contains the series variable
            $seriesVariable, // The name of variable whose values will make series
            $axisDim, // The dimension which contains the variable which values will appear on the axis
            $axisVariable           // The variable whicn contains the 
    ) {
        // Example : 'Tasks', 'status', 'count', 'department', 'quarter'
        // 1. Find the column names for the dimenions
        // 3. Measurement

        /* $sql = "SELECT count(a.status) as measure, b.quarter, c.department
          from BL_Tasks a,
          INNER JOIN Dim_Time b ON b.dimId = a.Dim_Time
          INNER JOIN Dim_Organization c on c.dimId = a.Dim_Organization
          GROUP BY Dim_Time, Dim_Organization"; */

        $sql = "SELECT $aggregationType(a.status) as measure, b.$axisVariable, c.$seriesVariable
               from BL_Tasks a, 
               INNER JOIN $axisDim b ON b.dimId = a.$axisDim
               INNER JOIN $seriesDim c on c.dimId = a.$seriesDim
               ORDER BY $seriesDim 
               GROUP BY $axisDim, $seriesDim";

        $stmt = $this->ldwhConnection->prepare($sql);

        if ($stmt == false) {
            print "Error in the sql :" . $this->ldwhConnection->error;
        }

        $stmt->bind_result($measure, $axisVariable, $seriesVariable);
        $lastSeriesVariabe = 'NULL';

        while ($stmt->fetch()) {

            if ($seriesVariable != $lastSeriesVariable) {
                $this->data->series[$seriesVariable] = array('name' => $seriesVariable, 'data' => array([$measure]));
            } else {
                $this->data->series[$seriesVariable]['data'][] = $measure;
            }
        }
    }

}

?>