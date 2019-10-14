import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { DescribeTableComponent } from './describe.component';

describe('DescribeTableComponent', () => {
  let component: DescribeTableComponent;
  let fixture: ComponentFixture<DescribeTableComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ DescribeTableComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(DescribeTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
