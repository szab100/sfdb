import { async, ComponentFixture, TestBed } from '@angular/core/testing';

import { ViewTableComponent } from './view.component';

describe('ViewComponent', () => {
  let component: ViewTableComponent;
  let fixture: ComponentFixture<ViewTableComponent>;

  beforeEach(async(() => {
    TestBed.configureTestingModule({
      declarations: [ ViewTableComponent ]
    })
    .compileComponents();
  }));

  beforeEach(() => {
    fixture = TestBed.createComponent(ViewTableComponent);
    component = fixture.componentInstance;
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });
});
