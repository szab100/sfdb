/**
 * @fileoverview This file defines controller for listing database tables.
 */

app.controller("TableListController",
  [
    '$scope', '$http', '$location', '$rootScope',
    function($scope, $http, $location, $rootScope) {
      $scope.tables = [];

      $scope.showTables = function() {
        if (!$rootScope.db) {
          return
        }

        $http.get('/api/' + $rootScope.db.name)
          .then(function (resp) {
            $scope.tables = resp.data;
        });
      }

      $scope.createTable = function() {
        if (!$rootScope.db) {
          return
        }

        $location.path("create_table");
      }

      $scope.deleteTable = function(table_index) {
        var err = "";
        if (!$scope.tables || table_index > $scope.tables.length) {
          err = "Invalid table selected";
          return;
        }

        var table_name = $scope.tables[table_index];

        $http.delete('/api/' + $rootScope.db.name + '/' + table_name)
          .then(function (resp) {
            $scope.showTables();
        });
      }

      $scope.showTables();
    }
  ]
);