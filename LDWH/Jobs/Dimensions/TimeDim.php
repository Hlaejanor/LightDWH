<?php

/*
 * TIME DIMENSION
 * All dimension have three function which you must implement : 
 * 1. Create dimension table IF NOT EXISTS
 * 2. GetCoord - identifies a unique combination of dimension variables
 * 3. InsertCoord - inserts a new unique combination of dimension variables
 * 
 * In this example, the time dimension contains
 * 
 */

class TimeDim extends Dimension {
 
    function __construct() {
        parent::__construct('Time');
        $this->dimensonName = 'Time';
        $this->variables = ['quarter', 'month', 'dayofmonth', 'weekday'];
        $this->createDimensionTable();
        $this->ensureVariableMapping();
        return true;
    }

    /*
     * Please make sure that the dimension table exists
     */

    function createDimensionTable() {
        $sql = "CREATE TABLE IF NOT EXISTS `Dim_$this->dimensionName`  (
        `dimId` int(11) unsigned NOT NULL AUTO_INCREMENT,
        `current` int(11) NOT NULL DEFAULT '0',
        `quarter` varchar(30) DEFAULT NULL,
        `month` varchar(30) DEFAULT NULL,
        `dayofmonth` varchar(30) DEFAULT NULL,
        `weekday` varchar(30) DEFAULT NULL,
      `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (`dimId`),
        UNIQUE KEY `current` (`current`,`quarter`,`month`,`dayofmonth`,`weekday`)
      ) ENGINE=MyISAM DEFAULT CHARSET=utf8;";

        $stmt = $this->ldwhConnection->prepare($sql);

        if ($stmt == false) {
            print "Error in sql : " . $this->ldwhConnection->error;
        }
        $stmt->execute();
        if ($stmt->errno > 0) {
            print "There was an error when creating the DImensionTabe. " . $stmt->error;
            return false;
        }
        return true;
    }

    /*
     * Please make sure that the variables
     * belonging to this dimension are listed in the dimensionMapping table
     * This is required for the 
     * 
     */

   

    /*
     * This wil return the coordinate id if the row exists in the dimension
     * Change the dimension variables when developing new dimensions.
     */

    function getCoord($variables) {
       
        $sql = "SELECT dimId from Dim_$this->dimensionName 
           WHERE  month = ? 
           AND  quarter = ?
           AND dayofmonth = ?
           AND weekday = ? ";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === FALSE) {
            print 'Error in sql:' . $this->ldwhConnection->error;
        }

        $stmt->bind_param(
                'ssss', $variables['month'], $variables['quarter'], $variables['dayofmonth'], $variables['weekday']);

        $stmt->execute();
        $stmt->store_result();
        $stmt->bind_result($dimId);
        if ($stmt->num_rows == 0) {
            return $this->insertCoord($variables);
        }
        return $dimId;
    }

    /*
     *  Insert a coordiante into the dimension table and return the id
     */

    function insertCoord($variables) {
        $sql = "INSERT INTO Dim_$this->dimensionName (
                 month, 
                 quarter, 
                 dayofmonth, 
                 weekday) 
                 values (?,?,?,?)";
   
        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === FALSE) {
            print 'Error in sql:' . $this->ldwhConnection->error;
        }
        
    
        $stmt->bind_param
                ('ssss', $variables['month'], $variables['quarter'], $variables['dayofmonth'], $variables['weekday']
        );
        $stmt->execute();
        $stmt->store_result();
        if ($stmt->insert_id > 0) {
            return $stmt->insert_id;
        } else {
            print "Error inserting id ";
            print_r($variables);
            return false;
        }
    }

}

?>