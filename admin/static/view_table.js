/**
 * @fileoverview This file defines controller for listing table contents.
 */


app.controller("ViewTableController",
[
  '$scope', '$http', '$location', '$rootScope',
  function($scope, $http, $location, $rootScope) {
    $scope.table = null;

    $scope.goBack = function() {
      $location.path("table_list")
    }

    $scope.showTableRows = function() {
      var err = "";

      if (!$rootScope.db) {
        err = "No connection to DB";
        return
      }

      var searchObject = $location.search();
      if (!searchObject || !searchObject.hasOwnProperty('table_name')) {
        return;
      }

      $scope.table_name = searchObject['table_name']
      $http.get('/api/' + $rootScope.db.name + "/" + $scope.table_name)
        .then(function (resp) {
          $scope.table = resp.data;
      });
    }

    $scope.showTableRows();
  }
]
);