<?php

/*
 * TIME DIMENSION
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

class OrganizationDim extends Dimension {

    function __construct() {
        parent::__construct('Organization');
        $this->dimensionName = 'Organization';
        
        $this->variables = ['name', 'department', 'office'];
        $this->createDimensionTable();
        $this->ensureVariableMapping();
        return true;
    }

    function createDimensionTable() {
        
        $sql = "CREATE TABLE IF NOT EXISTS `Dim_$this->dimensionName` (
  `dimId` int(11) unsigned NOT NULL AUTO_INCREMENT,
  `current` int(11) NOT NULL DEFAULT '0',
  `name` varchar(30) DEFAULT NULL,
  `department` varchar(30) DEFAULT NULL,
  `office` varchar(30) DEFAULT NULL,
  `created` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (`dimId`),
  UNIQUE KEY `current` (`current`,`name`,`department`,`office`)
) ENGINE=MyISAM AUTO_INCREMENT=4 DEFAULT CHARSET=utf8";

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
     * This wil return the coordinate id if the row exists in the dimension
     * Change the dimension variables 
     */

    function getCoord($variables) {
        $sql = "SELECT dimId from Dim_$this->dimensionName 
           WHERE  name = ? 
           AND  department = ?
           AND office = ?";


        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === FALSE) {
            print 'Error in sql:' . $this->ldwhConnection->error;
        }

        $stmt->bind_param(
                'sss', 
                $variables['name'], $variables['department'], $variables['office']
        );

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
                 name, department, office )
                values (?,?,?)";
        
        $stmt = $this->ldwhConnection->prepare($sql);
        
        if ($stmt === FALSE) {
            print 'Error in sql:' . $this->ldwhConnection->error;
        }
        
        $stmt->bind_param
                ('sss', // Same number of s's as question marks in query
                strval($variables['name']),
                strval($variables['department']),
                strval($variables['office'])
        );
        $stmt->execute();
        $stmt->store_result();
        if ($stmt->insert_id > 0) {
            return $stmt->insert_id;
        } else {
            print "Error inserting id";
            return false;
        }
    }

}

?>