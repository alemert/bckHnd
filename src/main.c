/******************************************************************************/
/*                                                                            */
/*            H A N D L E   M Q   B A C K O U T   M E S S A G E S             */
/*                                M A I N . C                                 */
/*                                                                            */
/******************************************************************************/

/******************************************************************************/
/*   I N C L U D E S                                                          */
/******************************************************************************/

// ---------------------------------------------------------
// system
// ---------------------------------------------------------
#include <stdlib.h>

// ---------------------------------------------------------
// MQ
// ---------------------------------------------------------

// ---------------------------------------------------------
// own 
// ---------------------------------------------------------
//#include "main.h"
#include "bckhnd.h"
#include "sighnd.h"
#include <ctl.h>
#include <msgcat/lgstd.h>
#include <cmdln.h>

// ---------------------------------------------------------
// local
// ---------------------------------------------------------

/******************************************************************************/
/*   D E F I N E S                                                            */
/******************************************************************************/
#define DEFAULT_LOG_DIR  "/var/mqm/errors/appl"
#define DEFAULT_LOG_FILE "bckhnd.log" 

/******************************************************************************/
/*   M A C R O S                                                              */
/******************************************************************************/

/******************************************************************************/
/*   P R O T O T Y P E S                                                      */
/******************************************************************************/

/******************************************************************************/
/*                                                                            */
/*                                  M A I N                                   */
/*                                                                            */
/******************************************************************************/
#ifndef __TDD__

int main(int argc, const char* argv[] )
{
  int sysRc ;

  sysRc = handleCmdLn( argc, argv ) ;
  if( sysRc != 0 ) goto _door ;


  int logLevel = logStr2lev( (const char*)getStrAttr("lev"));
  if(logLevel == LNA ) logLevel = LOG ;

  sysRc = initLogging( DEFAULT_LOG_DIR"/"DEFAULT_LOG_FILE, logLevel );
  if( sysRc != 0 )
  {
    fprintf(stderr,"can not init logging");
    goto _door ;
  }

  initSignal();

  sysRc = backoutHandler();
  if( sysRc != 0 )
  {
    goto _door;
  }

_door :

  logger(LSTD_PRG_STOP,"bckhnd");

  if( sysRc == 0 ) exit(0);
  exit(1);
}

#endif

/******************************************************************************/
/*                                                                            */
/*   F U N C T I O N S                                                        */
/*                                                                            */
/******************************************************************************/

