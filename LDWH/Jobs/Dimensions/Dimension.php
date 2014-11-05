<?php

/* 
 * ABSTRACT DIMENSION CLASS
 * This class acts contains the function 
 * 
 * 
 * In this example, the time dimension contains
 * 
 */

abstract class Dimension{
    public $ldwhConnection;
    public $dimensionName;
    public $variables;
    
    function __construct($dimensionName){
      
        $this->ldwhConnection =  new mysqli(LDWHServer, LDWHuser, LDWHPass, LDWHDb);
        
        $this->dimensionName = $dimensionName;
        
        return true;
    }
    
    /*
     * Please make sure that the dimension table exists
     */
    
    abstract function createDimensionTable();
        // Abstract class - dimensions must implement this method
    
    
    /*
     * Please make sure that the variables
     * belonging to this dimension are listed in the dimensionMapping table
     * This is required for the 
     * 
     */
    
    
     function ensureVariableMapping() {
        $this->ldwhConnection =  new mysqli(LDWHServer, LDWHuser, LDWHPass, LDWHDb);
        $sql = "INSERT INTO dimensionMapping (dimensionName, variable) values (?, ?)";
        $stmt = $this->ldwhConnection->prepare($sql);
        
        if ($stmt == false) {
            print "Error in sql : " . $this->ldwhConnection->error;
        }
      
        $value = null;
        $stmt->bind_param('ss', $this->dimensionName, $value);
        
        foreach ($this->variables as  $value) {
         
            $stmt->execute();
        }
    }
    
   /*
    * This wil return the coordinate id if the row exists in the dimension
    * Change the dimension variables when developing new dimensions.
    */
   
   abstract function getCoord($variables);
   
   
   /*
    *  Insert a coordiante into the dimension table and return the id
    */
   
   abstract function insertCoord($variables);
}

?>