/**
 * @fileoverview This file defines controller for listing table contents.
 */


app.controller("ViewTableController",
[
  '$scope', '$http', '$location', '$rootScope',
  function($scope, $http, $location, $rootScope) {
    $scope.table = null;
    $scope.table_name = "";
    $scope.err = "";
    $scope.hide_edit = true;
    $scope.edit = {columns: '', values: {}, mode: 'insert', original_row: {}};

    $scope.goToTableList = function() {
      $location.path("table_list")
    }

    $scope.showTableRows = function() {
      if (!$rootScope.db) {
        $scope.err = "No connection to DB";
        return
      }

      var searchObject = $location.search();
      if (!searchObject || !searchObject.hasOwnProperty('table_name')) {
        $scope.err = "Invalid table requested";
        return;
      }

      $scope.table_name = searchObject['table_name']
      $http.get('/api/' + $rootScope.db.name + "/" + $scope.table_name)
      .then(function (resp) {
          $scope.table = resp.data;
          $scope.edit.columns = $scope.table.columns
      }, function(err) {
        $scope.err = "Failed to retrieve table rows";
      });
    }

    $scope.toggleEditForm = function() {
      $scope.hide_edit = !$scope.hide_edit
    }

    $scope.insertSingleRow = function() {
      $scope.insertRow();
      $scope.hide_edit = true;
    }

    $scope.insertRow = function() {
      if (!$scope.table_name) {
        $scope.err = "Table name not specified";
      }

      if (!$scope.edit.values) {
        $scope.err = "Values for fields are not provided";
      }

      var insert = "INSERT INTO " + $scope.table_name;
      var cols = "";
      var vals = "";

      Object.keys($scope.edit.values).forEach(function(key){
        cols += (cols == "" ? "" : ", ") + key;
        vals += (vals == "" ? "" : ", ");
        if ($scope.table.column_types[key] === "int") {
          vals += parseInt($scope.edit.values[key], 10).toString();
        } else if ($scope.table.column_types[key] === "bool") {
          vals += parseInt($scope.edit.values[key], 10) == 0 ? false : true;
        } else {
          vals += '"' + $scope.edit.values[key].toString() + '"';
        }
      });

      $http.post('/api/' + $rootScope.db.name + "/exec",
        insert + "(" + cols + ") VALUES (" + vals + ");",
        { headers: { 'Content-Type': "text/plain" } })
      .then(function (resp) {
        $scope.edit.values = {}
        $scope.showTableRows();
      }, function(err) {
        $scope.err = "Failed to insert row";
      });
    }

    $scope.updateRow = function() {
      if (!$scope.table_name) {
        $scope.err = "Table name not specified";
      }

      if (!$scope.edit.values) {
        $scope.err = "Values for fields are not provided";
      }

      var update = "UPDATE " + $scope.table_name;
      var set_vals = "";
      var where_cond = "";

      Object.keys($scope.edit.values).forEach(function(key){
        set_vals += (set_vals == "" ? "" : ", ");
        set_vals += key + " = ";
        if ($scope.table.column_types[key] === "int") {
          set_vals += parseInt($scope.edit.values[key], 10).toString();
        } else if ($scope.table.column_types[key] === "bool") {
          set_vals += parseInt($scope.edit.values[key], 10) == 0 ? false : true;
        } else {
          set_vals += '"' + $scope.edit.values[key].toString() + '"';
        }
      });

      // Generate where clause for original row
      Object.keys($scope.table.columns).forEach(function(key){
        where_cond += (where_cond == "" ? "" : " AND ");
        var column_name = $scope.table.columns[key];
        where_cond += column_name + " = ";
        if ($scope.table.column_types[column_name] === "int") {
          where_cond += $scope.edit.original_row[key].toString();
        } else if ($scope.table.column_types[column_name] === "bool") {
          where_cond += $scope.edit.original_row[key] ? false : true;
        } else {
          where_cond += '"' + $scope.edit.original_row[key].toString() + '"';
        }
      })

      $http.post('/api/' + $rootScope.db.name + "/exec",
      update + " SET " + set_vals + " WHERE " + where_cond + ";",
        { headers: { 'Content-Type': "text/plain" } })
      .then(function (resp) {
        $scope.edit.values = {}
        $scope.edit.original_row = {}
        $scope.hide_edit = true;
        $scope.showTableRows();
      }, function(err) {
        $scope.err = "Failed to insert row";
      });
    }

    $scope.showUpdateForm = function(row_index) {
      // Copy values also into original_row to generate edit statement
      $scope.edit.original_row = $scope.table.rows[row_index];

      Object.keys($scope.table.columns).forEach(function(index){
        var column_name = $scope.table.columns[index]
        var column_value = $scope.table.rows[row_index][index];
        $scope.edit.values[column_name] = column_value;
      });

      $scope.edit.mode = 'update';
      $scope.hide_edit = false;
    }

    $scope.showInsertForm = function() {
      $scope.edit.mode = 'insert';
      $scope.hide_edit = false;
    }

    $scope.showTableRows();
  }
]
);