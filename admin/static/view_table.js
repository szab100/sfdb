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
    $scope.edit = {columns: '', mode: 'insert'};

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
        vals += (vals == "" ? "" : ", ") + '"' + $scope.edit.values[key] + '"';
      });

      $http.post('/api/' + $rootScope.db.name + "/exec",
        insert + "(" + cols + ") VALUES (" + vals + ");",
        { headers: { 'Content-Type': "application/json" } })
      .then(function (resp) {
        $scope.showTableRows();
      }, function(err) {
        $scope.err = "Failed to insert row";
      });
    }

    $scope.showTableRows();
  }
]
);