<?php

/* 
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

class Transform extends LDWH{

     public $map;
     public $dimensionVariables;
     
         
    function __construct(){    
        parent::__construct();
        
        $this->map = array();
        return true;
    }
    
    /* Map the variables to dimensions
     * This will tell the population of the Business layer which variables
     * to send to the Dimension Tables
     */
    
    
    function getDimensionMapping(){
  
    	$sql = 'SELECT variable, dimensionName form dimensionMapping ORDER by dimensionName';
        $this->ldwhConneciton->prepare($sql);
	$stmt->bind_result($variable, $dimensionName);
        $variables = array();
        $lastDimensionName = '';
        while ($stmt->fetch()) {
            if($lastDimensionName != $dimensionName){
                $this->dimensionVariables[$dimensionName] = $array();
            }
            $this->dimensionVariables[$dimensionName][$variable] = null;
        }
        
        return true;
    }
    
    /* 
     * Get the table column names.
     * Make a list of variables that belong in each dimension
    
    
    function getTableMetadata(){
       $sql = "SELECT 
           COLUMN_NAME, 
           DATA_TYPE 
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE table_name = T_'$this->tableName'";
     
       $stmt = $this->prepare($sql);
       $stmt->execute();
       $stmt->bind_result($variable, $type);
       $stmt->store_result();
       
       // For each variable in the table, find if the variable is a fact (measure)
       // or a dimension variable
       
       while ($stmt->fetch()){ // for each row
           if($dim = $this->getDim($variable)){ // get the dimension 
               $this->dimensionVariables[$dim][$variable] = null; // add ther variable to a dimension array 
           }
       }
       $this->dimensionVariables = ksort($this->dimensionVariables); // Sort the array, so that all the variables for each dimension 
       return true;
    }
     *  */
     
    /*
     * Get the dim mapped to a variable name
 
    function getDim($variable){
       if( array_key_exists($name, $this->map)){
           return $this->map[$name];
       }
       else{
           return false;
       }
    }    */
     /*
     * Get the variables mapped to a dimension
     */
    function getVariables($dimName){
        foreach ($this->dim as $variable => $dimension){
            if ($dimension == $dimName){
                $arr[] = $name;
            }
        }
        return $arr;
    }
    
    /*
     * Create the transformation for this transformation into a temporary table
     */
    function transform(){
        $sql = "DROP TABLE IF EXISTS T_tasks;
        CREATE TEMPORARY TABLE T_TASKS
        SELECT  id, userId, status, activity, completed, name, department, office, 
        DAYNAME(updated) as weekday, DAYOFMONTH(updated) as dayofmonth, monthname(updated) as month, QUARTER(updated) as quarter 
        FROM PL_$this->sourceTable;";
        
        $stmt = $this->prepare($sql);
        $stmt->execute();
      
        
    }
    
    /*
     *  Populate the transformation into the Business Layer.
     *  Replace the dimension variables with references to the dimension tables.
     *  Calls Upsert and insertDimensionRow();
     */
    
    function populate(){
        $sql = "SELECT   from 
        FROM PL_$this->sourceTable";
        
        $stmt = $this->ldwhConnection->prepare($sql);
        $stmt->execute();
        $stmt->store_result();
       
       // For each variable in the table, find if the variable is a fact (measure)
       // or a dimension variable.
       $dimensionCoord = array();
          

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === false) {
            print 'Error in sql : ' . $this->ldwhConnection->error;
        }
        $stmt->bind_result($)
        $stmt->execute();
        $this->end('DELTA LOAD');
    }
    }
    
    
    function upsert( $dimensionCoord , $facts ) {
       
        $sql = "INSERT INTO BL_$this->sourceTable
          (organizationDim, timeDim, id, userId, status, activity, completed)
          VALUES (?,?,?,?,?,?,?)";

        $stmt = $this->ldwhConnection->prepare($sql);
        if ($stmt === False) {
            print 'error n SQL :' . $this->ldwhConnection->error;
        }

        $stmt->bind_param('iiiiiii', 
                $dimensionCoord['organizationDim'],  
                $dimensionCoord['timeDim'],  
                $this->dimensionVariables['facts']['id'] ,
                $this->dimensionVariables['facts']['userId'] ,
                $this->dimensionVariables['facts']['status'] ,
                $this->dimensionVariables['facts']['activity'],
                $this->dimensionVariables['facts']['completed']);

        $stmt->execute();

        if ($stmt->errno > 0) {
            switch ($stmt->errno) {
                case 1062:
                    // The id already exists in the table and this does not allow new entries. We can copy 
                    // update the table, but let us archive the current version first.
                    
                    break;
                default :
                    $this->out('INSERT FACT error' . $stmt->error);
                    return $stmt->affected_rows;
                    break;
            }
        }
        
      function getCoord($dimensionName, $constraints){
          $sql = "SELECT fid FROM $dimensionName WHERE";
         
         
          
      }
       
    }
}
 
?>