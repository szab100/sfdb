import { Component, OnInit } from '@angular/core';
import { Router, ActivatedRoute } from '@angular/router';

import { HttpStatus } from '../services/constants';

@Component({
  selector: 'app-error',
  templateUrl: './error.component.html',
  styleUrls: ['./error.component.scss']
})
export class ErrorComponent implements OnInit {
  title: string;
  errorMessage: string;

  constructor(private route: ActivatedRoute, private router: Router) { }

  ngOnInit() {
    this.route.params.subscribe(params => {
      if (params['errorCode']) {
        this.updateErrorMessage(Number(params['errorCode']));
      }
    });
  }

  private updateErrorMessage(errorCode: number): void {
    switch (errorCode) {
      case HttpStatus.UNAUTHORIZED:
        this.title = 'Login required';
        this.errorMessage = `You are currently logged out or your authentication token has expired.
          Please login using the button on the top-right!`;
        break;
      case HttpStatus.FORBIDDEN:
        this.title = 'Unauthorized access';
        this.errorMessage = 'You are not authorized to access the selected resource.';
        break;
      case HttpStatus.REQUEST_TIMEOUT:
        this.title = 'Request timed out';
        this.errorMessage = 'Your previous request has timed out. Please try again.';
        break;
      case HttpStatus.API_DOWN:
        this.title = 'SFDB API is unreachable';
        this.errorMessage = 'We couldn\'t reach SFDB backend API. Please make sure it is running and try again.';
        break;
      default:
        this.title = 'Something went wrong.';
        this.errorMessage = 'A generic error has happened';
    }
  }
}
