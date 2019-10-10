var app = angular
  .module('sfdb', ['ngMaterial', 'ngMessages', 'ngRoute'])
  .run(function($rootScope) {
    $rootScope.db = null
  })
  .config(function($routeProvider) {
    $routeProvider
      .when('/home', {
        templateUrl : 'home.html',
        controller : 'HomeController'
      })
      .when('/table_list', {
        templateUrl : 'table_list.html',
        controller : 'TableListController'
      })
      .when('/create_table', {
        templateUrl : 'create_table.html',
        controller : 'CreateTableController'
      })
      .when('/view_table', {
        templateUrl : 'view_table.html',
        controller : 'ViewTableController'
      })
      .otherwise({redirectTo: '/home'});
    });
