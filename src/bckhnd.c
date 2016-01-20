/******************************************************************************/
/*                                                                            */
/*            H A N D L E   M Q   B A C K O U T   M E S S A G E S             */
/*                              B C K H N D . C                               */
/*                                                                            */
/* -------------------------------------------------------------------------- */
/*                                                                            */
/*  functions:                  */
/*    - backoutHandler                  */
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
#include <cmqc.h>

// ---------------------------------------------------------
// own 
// ---------------------------------------------------------
#include <ctl.h>
#include <mqbase.h>

// ---------------------------------------------------------
// local
// ---------------------------------------------------------
#include <bckhnd.h>
#include <cmdln.h>

/******************************************************************************/
/*   G L O B A L S                                                            */
/******************************************************************************/

/******************************************************************************/
/*   D E F I N E S                                                            */
/******************************************************************************/

/******************************************************************************/
/*   M A C R O S                                                              */
/******************************************************************************/

/******************************************************************************/
/*   P R O T O T Y P E S                                                      */
/******************************************************************************/

/******************************************************************************/
/*                                                                            */
/*   F U N C T I O N S                                                        */
/*                                                                            */
/******************************************************************************/

/******************************************************************************/
/*  back out handler                    */
/******************************************************************************/
int backoutHandler()
{
  logFuncCall() ;               

  int sysRc = 0 ;

  MQHCONN hCon  ;                   // connection handle   
  char qmgrName[MQ_Q_MGR_NAME_LENGTH+1];
                                    //
  MQHOBJ  hBoq ;                    // queue handle   
  MQOD    dBoq = {MQOD_DEFAULT};    // queue descriptor
  char boqName[MQ_Q_NAME_LENGTH+1]; //
                              //
  MQHOBJ  hSrcq ;                    // queue handle   
  MQOD    dSrcq = {MQOD_DEFAULT};    // queue descriptor
  char srcqName[MQ_Q_NAME_LENGTH+1]; //
                            //
  MQHOBJ  hFwdq ;                    // queue handle   
  MQOD    dFwdq = {MQOD_DEFAULT};    // queue descriptor
  char fwdqName[MQ_Q_NAME_LENGTH+1]; //

  // -------------------------------------------------------
  // connect to queue manager 
  // -------------------------------------------------------
  memset( qmgrName, ' ', MQ_Q_MGR_NAME_LENGTH );  
  qmgrName[MQ_Q_MGR_NAME_LENGTH] = '\0'  ;  // copy queue manager name
  memcpy( qmgrName                       ,  // from command line attributes
          getStrAttr( "qmgr" )           ,  // to vara
          strlen( getStrAttr( "qmgr" ) ));  //
                                            //
  sysRc =  mqConn( (char*) qmgrName,        // queue manager          
                           &hCon  );        // connection handle            
                                            //
  switch( sysRc )                           //
  {                                         //
    case MQRC_NONE :     break ;            // OK
    case MQRC_Q_MGR_NAME_ERROR :            // queue manager does not exists
    {                                       //
      logger(LMQM_UNKNOWN_QMGR,qmgrName);   //
      goto _door;                           //
    }                                       //
    default : goto _door;                   // error will be logged in mqConn
  }                                         //
                                            //
  // -------------------------------------------------------
  // open back out queue
  // -------------------------------------------------------
  memset( boqName, ' ', MQ_Q_NAME_LENGTH );   //
  boqName[MQ_Q_NAME_LENGTH] = '\0' ;          //
  memcpy( dBoq.ObjectName   ,                 //
          boqName           ,                 //
          MQ_Q_NAME_LENGTH );                 //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dBoq                 , // queue descriptor
                      MQOO_INPUT_EXCLUSIVE  | 
                      MQOO_SET              |
                      MQOO_FAIL_IF_QUIESCING, // open options
                      &hBoq );                // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // open (original) source queue
  // -------------------------------------------------------
  memset( srcqName, ' ', MQ_Q_NAME_LENGTH );  //
  boqName[MQ_Q_NAME_LENGTH] = '\0' ;          //
  memcpy( dSrcq.ObjectName  ,                 //
          srcqName          ,                 //
          MQ_Q_NAME_LENGTH );                 //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dSrcq                , // queue descriptor
                      MQOO_INPUT_EXCLUSIVE  | 
                      MQOO_SET              |
                      MQOO_FAIL_IF_QUIESCING, // open options
                      &hSrcq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // open (original) source queue
  // -------------------------------------------------------
  memset( fwdqName, ' ', MQ_Q_NAME_LENGTH );  //
  boqName[MQ_Q_NAME_LENGTH] = '\0' ;          //
  memcpy( dFwdq.ObjectName  ,                 //
          fwdqName          ,                 //
          MQ_Q_NAME_LENGTH );                 //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dFwdq                , // queue descriptor
                      MQOO_INPUT_EXCLUSIVE  | 
                      MQOO_SET              |
                      MQOO_FAIL_IF_QUIESCING, // open options
                      &hFwdq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  _door:

  return sysRc ;
}
