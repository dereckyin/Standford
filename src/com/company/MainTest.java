package com.company;

import org.testng.annotations.Test;


public class MainTest {

    @Test
    public void testMain() throws Exception {

        Main main = new Main();

        int nCount = main.CountStringOccurrences("台上一台分鐘，台下十年功", "台");

        System.out.println(nCount);
    }
}