/***************************************************************************
                          main.cpp
                             -------------------
    copyright            : (C) 2013 Montanati, Paganelli, A. Bulgarelli, 
    					 : A. Zoli
    email                : bulgarelli@iasfbo.inaf.it, zoli@iasfbo.inaf.it

 ***************************************************************************/

/***************************************************************************
 *                                                                         *
 *   This program is free software; you can redistribute it and/or modify  *
 *   it under the terms of the GNU General Public License as published by  *
 *   the Free Software Foundation; either version 2 of the License, or     *
 *   (at your option) any later version.                                   *
 *                                                                         *
 ***************************************************************************/

#ifdef HAVE_CONFIG_H
#include <config.h>
#endif

#include <iostream>
#include <iomanip>
#include <string>
#include <sstream>
#include "InputFileFITS.h"

#include <sqlite3.h>

//file types
#define EVT 1
#define LOG 2

//columns of EVT file
#define EVT_TIME 0
#define EVT_PHI 2
#define EVT_RA 3
#define EVT_DEC 4
#define EVT_ENERGY 5
#define EVT_PH_EARTH 14
#define EVT_THETA 1
//I
#define EVT_PHASE 15 
//I
#define EVT_EVSTATUS 13 

//columsn of LOG file
#define LOG_TIME 0
#define LOG_PHASE 5
#define LOG_LIVETIME 39
#define LOG_LOG_STATUS 40
#define LOG_MODE 4
#define LOG_ATTITUDE_RA_Y 13
#define LOG_ATTITUDE_DEC_Y 14
#define LOG_EARTH_RA 24
#define LOG_EARTH_DEC 25
#define LOG_Q1 9
#define LOG_Q2 10
#define LOG_Q3 11
#define LOG_Q4 12

using namespace std;
using namespace qlbase;

bool tableExists(sqlite3* db, string table)
{
	sqlite3_stmt *stmt;
	stringstream ss;
	ss << "pragma table_info(" << table << ");";
	int rc = sqlite3_prepare_v2(db, ss.str().c_str(), -1, &stmt, NULL);
	if(rc != SQLITE_OK)
	{
		cerr << "Error on sqlite3_prepare_v2: " << sqlite3_errmsg(db) << endl;
		sqlite3_close(db);
		return false;
	}

	rc = sqlite3_step(stmt);
	if(rc != SQLITE_DONE && rc != SQLITE_ROW)
	{
		cerr << "Error on sqlite3_step: " << sqlite3_errmsg(db) << endl;
		sqlite3_finalize(stmt);
		return false;
	}

	bool retval = false;
	if (sqlite3_data_count(stmt) > 0)
		retval = true;

	sqlite3_finalize(stmt);

	return retval;
}

static int printCallback(void *data, int argc, char **argv, char **azColName)
{
	for(int i=0; i<argc; i++)
	{
		string value = "NULL";
		if(argv[i]) 
			value = argv[i];

		cout << string(azColName[i]) << " = " << value << endl;
	}
	cout << endl;

	return 0;
}

int sqlite_exec(sqlite3 *db, string sql) {
		char* zErrMsg = 0;
		int ret = sqlite3_exec(db, sql.c_str(), NULL, 0, &zErrMsg);
		if(ret != SQLITE_OK)
		{
			cout << "Sql \"" << sql << "\" failed." << endl;
			cout << "Error: " << zErrMsg << endl;
			sqlite3_free(zErrMsg);
			exit(0);
		}
}

bool myisnan(double var) {
	double d = var;
	return d != d;
}

///Import AGILE LOG and EVT files into SQLlite3
int main(int argc, char** argv) {

	//cout << "gtImporterELsqllite"<<endl;

	if(argc == 1) {
		cerr << "Please, provide (1) the fits file to import (2 optional) the last line to be read" << endl;
		return 0;
	}

	string filename = argv[1];
	InputFileFITS* inputFF;


	//open input file FITS
	try{
		inputFF = new InputFileFITS();
		inputFF->open(filename);
		inputFF->moveToHeader(1); //CHECK base index AZ

		//check input file fits type: EVT or LOG
		int ncols = inputFF->getNCols();
		int nrows_start = 0;
		int nrows_end = inputFF->getNRows();
		
		if(argc == 3) {
			nrows_end = atoi(argv[2]);
		}
		int type;

		if(ncols == 19) type = EVT;
		if(ncols == 41) type = LOG;

		// Create sqlite db file and table.
		string dbfilename("agile.db");
		

		sqlite3 *db;
		int ret = sqlite3_open(dbfilename.c_str(), &db);
		if(ret != SQLITE_OK)
		{
			cout << "Error " << ret << " while opening " << dbfilename;
			exit(0);
		}

		

		//type = EVT;
		//nrows_end = 10;
		if(type == EVT)
		{
		
			string tablename("EVT");
			if(!tableExists(db, tablename))
			{
				cout << "Table " << tablename << " not present. Creating.." << endl;

				string sql = "CREATE TABLE " + tablename + "(" \
				"TIME REAL PRIMARY KEY NOT NULL," \
				"THETA        REAL," \
				"PHI            REAL," \
				"RA        REAL," \
				"DEC        REAL," \
				"ENERGY        REAL," \
				"PH_EARTH        REAL," \
				"PHASE        INTEGER," \
				"EVSTATUS         INTEGER );";
			
				ret = sqlite_exec(db, sql);
				
				//creazione dell'indice
				//TODO
				
				cout << "Table created succesfully." << endl;
				cout << "Restart." << endl;
				exit(0);
			}
			
			//read all columns
			cout << "Read EVT file " << endl;
			std::vector<double> time = inputFF->read64f(EVT_TIME, nrows_start, nrows_end-1);
			std::vector<float> phi = inputFF->read32f(EVT_PHI, nrows_start, nrows_end-1);
			std::vector<float> ra = inputFF->read32f(EVT_RA, nrows_start, nrows_end-1);
			std::vector<float> dec = inputFF->read32f(EVT_DEC, nrows_start, nrows_end-1);
			std::vector<float> energy = inputFF->read32f(EVT_ENERGY, nrows_start, nrows_end-1);
			std::vector<float> ph_earth = inputFF->read32f(EVT_PH_EARTH, nrows_start, nrows_end-1);
			std::vector<float> theta = inputFF->read32f(EVT_THETA, nrows_start, nrows_end-1);
			std::vector<int16_t> phase = inputFF->read16i(EVT_PHASE, nrows_start, nrows_end-1);
			std::vector< std::vector<char> > status = inputFF->readString(EVT_EVSTATUS, nrows_start, nrows_end-1, 1);
			std::vector<int16_t> status2(nrows_end-nrows_start+1);

			for(uint32_t i  = 0; i<nrows_end; i++) {
				std::string evt;
				evt = &(status[i])[0];
				//cout << evt << endl;
				if(evt.compare("G") == 0) status2[i] = 0;
				if(evt.compare("L") == 0) status2[i] = 1;
				if(evt.compare("S") == 0) status2[i] = 2;

			}

/*			//write data into BDB

			//Create the vector to store into BDB
			Astro::agileEvt evt;

			//Create the map
			AgileEvtMap dbEvt(connection, "DBAgileEvt");

			cout << "Start write into BDB" << endl;

			for(uint32_t i  = 0; i<nrows_end; i++) {
				//&(status[i])[0]
				cout << setiosflags(ios::fixed) << std::setprecision(6) << (double) time[i] << " " << status2[i] << endl;

				//Populate the vector
				evt.clear();
				evt.push_back((Ice::Double) status2[i]);
				evt.push_back((Ice::Double) phase[i]);
				evt.push_back((Ice::Double) theta[i]);
				evt.push_back((Ice::Double) ph_earth[i]);
				evt.push_back((Ice::Double) energy[i]);
				evt.push_back((Ice::Double) dec[i]);
				evt.push_back((Ice::Double) ra[i]);
				evt.push_back((Ice::Double) phi[i]);
				evt.push_back((Ice::Double) time[i]);

				//Execute write
				dbEvt.insert(make_pair(time[i],evt));

			}
*/
			// Write data into db.
			/*
			stringstream ss;
			for(uint32_t i  = 0; i<nrows_end; i++)
			{
				int f1 = i;
				string f2 = "Charlie Parker";
				int f3 = 18+i;
				string f4 = "bebop street";
				float f5= 10.0f;
				ss << "INSERT INTO " << tablename << "(ID,NAME,AGE,ADDRESS,SALARY) " << endl;
				ss << "VALUES (" << f1 << ", '" << f2 << "', " << f3 << ", '" << f4 << "', " << f5 << "); " << endl;
			}
			*/
			
			for(uint32_t i  = 0; i<nrows_end; i++)
			{
				stringstream ss;
				ss << "INSERT INTO " << tablename << "(TIME, THETA, PHI, RA, DEC, ENERGY, PH_EARTH, PHASE, EVSTATUS) ";
				
				if( myisnan((double)ra[i]) || myisnan((double)dec[i]) || myisnan((double)energy[i]) || myisnan((double)ph_earth[i]) || myisnan((double)theta[i]) || myisnan((double)phi[i]) || myisnan((double)phase[i]) )  {
					cout << i << " nan" << endl;
					continue;
				}
				
				ss << "VALUES (" << setprecision(15) <<  time[i] << ", " << setprecision(7) << theta[i] << ", " << phi[i] << ", " << ra[i] << ", " << dec[i] << ", " << energy[i] << ", " << ph_earth[i] << ", " << phase[i] << ", " << status2[i]  << "); " << endl;
				
				
				ret = sqlite_exec(db, ss.str());
				//cout << i << endl;
			}
			
			
				
			/*char* zErrMsg = 0;
			ret = sqlite3_exec(db, ss.str().c_str(), NULL, 0, &zErrMsg);
			if(ret != SQLITE_OK)
			{
				cout << "Sql \"" << ss.str() << "\" failed." << endl;
				cout << "Error: " << zErrMsg << endl;
				sqlite3_free(zErrMsg);
			}*/
			
			// Read table values.
			/*
			string sql = "SELECT * FROM EVT";
			char* zErrMsg = 0;
			ret = sqlite3_exec(db, sql.c_str(), printCallback, 0, &zErrMsg);
			if(ret != SQLITE_OK)
			{
				cout << "Sql \"" << sql << "\" failed." << endl;
				cout << "Error: " << zErrMsg << endl;
				sqlite3_free(zErrMsg);
			}
			*/
		}
		if(type == LOG)
		{
			string tablename("LOG");
			if(!tableExists(db, tablename))
			{
				cout << "Table " << tablename << " not present. Creating.." << endl;

				string sql = "CREATE TABLE " + tablename + "(" \
				"ID LONG UNIQUE     NOT NULL," \
				"TIME REAL PRIMARY KEY       NOT NULL," \
				"THETA        REAL," \
				"PHI            REAL," \
				"RA        REAL," \
				"DEC        REAL," \
				"ENERGY        REAL," \
				"PH_EARTH        REAL," \
				"PHASE        INTEGER," \
				"EVSTATUS         INTEGER );";
			
				ret = sqlite_exec(db, sql);
				
				cout << "Table created succesfully." << endl;
			}
			
			//read all columns
			cout << "Read LOG file " << nrows_end << endl;
			std::vector<double> time = inputFF->read64f(LOG_TIME, nrows_start, nrows_end-1);
			std::vector<int16_t> phase = inputFF->read16i(LOG_PHASE, nrows_start, nrows_end-1);
			std::vector<float> livetime = inputFF->read32f(LOG_LIVETIME, nrows_start, nrows_end-1);
			std::vector<int16_t> log_status = inputFF->read16i(LOG_LOG_STATUS, nrows_start, nrows_end-1);
			std::vector<int16_t> mode = inputFF->read16i(LOG_MODE, nrows_start, nrows_end-1);
			std::vector<double> attitude_ra_y = inputFF->read64f(LOG_ATTITUDE_RA_Y, nrows_start, nrows_end-1);
			std::vector<double> attitude_dec_y = inputFF->read64f(LOG_ATTITUDE_RA_Y, nrows_start, nrows_end-1);
			std::vector<double> log_earth_ra = inputFF->read64f(LOG_EARTH_RA, nrows_start, nrows_end-1);
			std::vector<double> log_earth_dec = inputFF->read64f(LOG_EARTH_DEC, nrows_start, nrows_end-1);
			std::vector<float> q1 = inputFF->read32f(LOG_Q1, nrows_start, nrows_end-1);
			std::vector<float> q2 = inputFF->read32f(LOG_Q2, nrows_start, nrows_end-1);
			std::vector<float> q3 = inputFF->read32f(LOG_Q3, nrows_start, nrows_end-1);
			std::vector<float> q4 = inputFF->read32f(LOG_Q4, nrows_start, nrows_end-1);

/*			//write data into BDB
			uint32_t count = 0;

			//Create the vector to store into BDB
			Astro::agileLog agileLog;

			//Create key
			Astro::agileLogKey key;

			//Create the map
			AgileLogMap DBLog(connection, "DBAgileLog");

			cout << "Start write into BDB" << endl;

			for(uint32_t i = 0; i<nrows_end; i++) {
				cout << setiosflags(ios::fixed) << std::setprecision(6) << (double) time[i] << " ";
				cout << attitude_ra_y[i] << endl;
				if(attitude_ra_y[i] != 0) count++;

				//Populate the vector
				agileLog.clear();
				agileLog.push_back((Ice::Double) q4[i]);
				agileLog.push_back((Ice::Double) q3[i]);
				agileLog.push_back((Ice::Double) q2[i]);
				agileLog.push_back((Ice::Double) q1[i]);
				agileLog.push_back((Ice::Double) log_earth_dec[i]);
				agileLog.push_back((Ice::Double) log_earth_ra[i]);
				agileLog.push_back((Ice::Double) attitude_dec_y[i]);
				agileLog.push_back((Ice::Double) attitude_ra_y[i]);
				agileLog.push_back((Ice::Double) mode[i]);
				agileLog.push_back((Ice::Double) log_status[i]);
				agileLog.push_back((Ice::Double) livetime[i]);
				agileLog.push_back((Ice::Double) phase[i]);
				agileLog.push_back((Ice::Double) time[i]);

				//Populate the key
				key.time = time[i];
				key.livetime = livetime[i];
				key.logStatus = log_status[i];
				key.mode = mode[i];
				key.phase = phase[i];

				//Execute write
				DBLog.insert(make_pair(key, agileLog));


			}

			cout << count << endl;
*/

			// Write data into db.
			stringstream ss;
			for(uint32_t i  = 0; i<nrows_end; i++)
			{
				int f1 = nrows_end+i;
				string f2 = "Charlie Parker log";
				int f3 = 18;
				string f4 = "bebop street log";
				float f5= 2.0f;
				ss << "INSERT INTO " << tablename << "(ID,NAME,AGE,ADDRESS,SALARY) " << endl;
				ss << "VALUES (" << f1 << ", '" << f2 << "', " << f3 << ", '" << f4 << "', " << f5 << "); " << endl;
				cout << i << " " << ss << endl;
			}

			char* zErrMsg = 0;
			ret = sqlite3_exec(db, ss.str().c_str(), NULL, 0, &zErrMsg);
			if(ret != SQLITE_OK)
			{
				cout << "Sql \"" << ss.str() << "\" failed." << endl;
				cout << "Error: " << zErrMsg << endl;
				sqlite3_free(zErrMsg);
			}
		}

	} catch(IOException* e) {
		cout << e->getErrorCode() << ": " << e->what() << endl;
		return e->getErrorCode();
	}
}
