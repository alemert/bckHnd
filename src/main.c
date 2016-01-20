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

// ---------------------------------------------------------
// MQ
// ---------------------------------------------------------

// ---------------------------------------------------------
// own 
// ---------------------------------------------------------
#include "main.h"
#include "bckhnd.h"
#include <ctl.h>
#include <msgcat/lgstd.h>

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


  int logLevel = DEFAULT_LOG_LEVEL ;   // ERR 
      logLevel = CRI ;

  sysRc = initLogging( DEFAULT_LOG_DIR"/"DEFAULT_LOG_FILE, logLevel );
  if( sysRc != 0 )
  {
    fprintf(stderr,"can not init logging");
    goto _door ;
  }

  sysRc = backoutHandler();
  if( sysRc != 0 )
  {
    goto _door;
  }

_door :

  return sysRc ;
}

#endif

/******************************************************************************/
/*                                                                            */
/*   F U N C T I O N S                                                        */
/*                                                                            */
/******************************************************************************/

