/******************************************************************************/
/*                                                                            */
/*            H A N D L E   M Q   B A C K O U T   M E S S A G E S             */
/*                              B C K H N D . C                               */
/*                                                                            */
/* -------------------------------------------------------------------------- */
/*                                                                            */
/*  functions:                                          */
/*    - backoutHandler                              */
/*    - moveMessages                      */
/*    - readMessage                        */
/*                                    */
/******************************************************************************/

/******************************************************************************/
/*   I N C L U D E S                                                          */
/******************************************************************************/

// ---------------------------------------------------------
// system
// ---------------------------------------------------------
#include <errno.h>
#include <stdlib.h>

// ---------------------------------------------------------
// MQ
// ---------------------------------------------------------
#include <cmqc.h>

// ---------------------------------------------------------
// own 
// ---------------------------------------------------------
#include <ctl.h>
#include <msgcat/lgstd.h>
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
#define INITIAL_MSG_SIZE 4096

/******************************************************************************/
/*   M A C R O S                                                              */
/******************************************************************************/

/******************************************************************************/
/*   P R O T O T Y P E S                                                      */
/******************************************************************************/
int moveMessages();
int readMessage( MQHCONN _hCon    ,  // connection handle   
                 MQHOBJ  _hGetQ   ,  // get queue handle
		 PMQMD   _pMd     ,  // message descriptor (set to default) 
                 PMQVOID _pBuffer ,
                 PMQLONG _pMsgLng );

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
  memset( dBoq.ObjectName, ' ', MQ_Q_NAME_LENGTH );   //
  dBoq.ObjectName[MQ_Q_NAME_LENGTH] = '\0' ;          //
  memcpy( dBoq.ObjectName   ,                 //
          getStrAttr("backout")           ,                 //
          strlen(getStrAttr("backout")) );                 //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dBoq                 , // queue descriptor
                      MQOO_INPUT_EXCLUSIVE  | // open for exclusive get
                      MQOO_FAIL_IF_QUIESCING, // fail if stopping queue manager
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
  memset( dSrcq.ObjectName, ' ', MQ_Q_NAME_LENGTH );  //
  dSrcq.ObjectName[MQ_Q_NAME_LENGTH] = '\0' ;          //
  memcpy( dSrcq.ObjectName  ,                 //
          getStrAttr("source")          ,                 //
          strlen( getStrAttr("source") )         );                 //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dSrcq                , // queue descriptor
                      MQOO_OUTPUT           | // open object for put
                      MQOO_SET_ALL_CONTEXT  | // keep original date/time in MQMD
                      MQOO_FAIL_IF_QUIESCING, // fail if stopping queue manager
                      &hSrcq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // open forward queue
  // -------------------------------------------------------
  memset( dFwdq.ObjectName, ' ', MQ_Q_NAME_LENGTH );  //
  dFwdq.ObjectName[MQ_Q_NAME_LENGTH] = '\0' ;          //
  memcpy( dFwdq.ObjectName  ,                 //
          getStrAttr("forward")          ,                 //
          strlen( getStrAttr("forward") )         );                 //
                                              //
  sysRc=mqOpenObject( hCon                  , // connection handle
                      &dFwdq                , // queue descriptor
                      MQOO_OUTPUT           | // open object for put
                      MQOO_SET_ALL_CONTEXT  | // keep original date/time in MQMD
                      MQOO_FAIL_IF_QUIESCING, // open options
                      &hFwdq );               // queue handle
                                              //
  switch( sysRc )                             //
  {                                           //
    case MQRC_NONE : break ;                  //
    default        : goto _door;              //
  }                                           //
                                              //
  // -------------------------------------------------------
  // move messages under sync point
  // -------------------------------------------------------
  sysRc = moveMessages( hCon, hBoq, hSrcq, hFwdq );

  _door:

  logFuncExit( );
  return sysRc ;
}

/******************************************************************************/
/*   move messages             */
/******************************************************************************/
int moveMessages( MQHCONN _hCon    ,     // connection handle   
                  MQHOBJ  _hGetQ   ,     // get queue handle
                  MQHOBJ  _hPutOrg ,     // put queue original handle 
                  MQHOBJ  _hPutFwd )     // get queue forward handle
{
  logFuncCall() ;               

  int sysRc = 0 ;
  MQLONG reason ;

  MQMD  md  = {MQMD_DEFAULT} ;    // message descriptor (set to default)
  MQGMO gmo = {MQGMO_DEFAULT};    // get message option set to default
//MQPMO pmo = {MQPMO_DEFAULT};    // put message option set to default
                                  //
                                  //
  static PMQVOID *buffer = NULL;  //
                                  //
  static MQLONG msgLng     = INITIAL_MSG_SIZE ;
  static MQLONG bufferSize = INITIAL_MSG_SIZE; 

  // -----------------------------------------------------
  // initialization of static vara with first call of this function
  // -----------------------------------------------------
  if( !buffer )                             //
  {                                         // message buffer, allocation 
    buffer = (PMQVOID) malloc( msgLng );    // necessary only on first call
    if( !buffer )                           // of this function, since static
    {                                       //
      logger( LSTD_MEM_ALLOC_ERROR );       //
      sysRc = errno ;                       //
      goto _door;                           //
    }                                       //
  }                                         //
                                            //
  // -----------------------------------------------------
  // start transaction
  // -----------------------------------------------------
  reason = mqBegin( _hCon );                // begin transaction
  switch( reason )                          //
  {                                         //
    case MQRC_NONE :                        //
    case MQRC_NO_EXTERNAL_PARTICIPANTS :    // transactions without external 
    {                                       //  resource manager
      sysRc = MQRC_NONE;                    //
      break;                                //
    }                                       //
    default : goto _door;                   //
  }                                         //
                                            // 
  // -----------------------------------------------------
  // read the message
  // -----------------------------------------------------
  sysRc = readMessage( _hCon    ,  // connection handle   
                      _hGetQ   ,  // get queue handle
		      &md     ,  // message descriptor (set to default) 
                      buffer ,  // ???? ACHTUNG u.U. muss zeiger auf buffer verwendet werden
                      &msgLng );
#if(0)
  memcpy( &md.MsgId         ,               // flash message id, 
          MQMI_NONE         ,               // 
          sizeof(md.MsgId) );               // 
  md.Version = MQMD_VERSION_2;              //
                                            //
  gmo.MatchOptions = MQMO_MATCH_MSG_ID;     // 
  gmo.Options      = MQGMO_CONVERT    +     //
                     MQGMO_SYNCPOINT  ,     //
                     MQGMO_WAIT       ;     //
  gmo.Version      = MQGMO_VERSION_3  ;     //
                                            //
  reason = mqGet( _hCon    ,                // connection handle
                  _hGetQ   ,                // pointer to queue handle
                  buffer   ,                // message buffer
                  &msgLng  ,                // buffer length
                  &md      ,                // message descriptor
                  gmo      ,                // get message option
                  60000   );                // wait interval in milliseconds
                                            // (makes 1Minute)
  switch( reason )                          //
  {                                         //
    // ---------------------------------------------------
    // read was OK
    // ---------------------------------------------------
    case MQRC_NONE :                        //
    {                                       //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // no message found -> OK
    // ---------------------------------------------------
    case MQRC_NO_MSG_AVAILABLE :            // no message available, necessary 
    {                                       //  for catching signals
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // an error occurred preventing later commit
    // ---------------------------------------------------
    case MQRC_BACKED_OUT:                   //
    {                                       //
      sysRc = reason;                       //
      reason = mqRollback( _hCon );         // roll back 
      if( reason != MQRC_NONE )             //
      {                                     //
        sysRc = reason;                     //
      }                                     //
      goto _door;                           //
    }                                       //
                                            //
    // ---------------------------------------------------
    // message buffer to small for the (physical) message 
    //   - rollback
    //   - resize buffer
    //   - re-read message
    // ---------------------------------------------------
    case MQRC_TRUNCATED_MSG_FAILED :        // 
    {                                       //
      logMQCall( WAR, "MQGET", reason );    //
                                            //
      sysRc = reason;                       //
      if( reason != MQRC_NONE )             //
      {                                     //
        sysRc = reason;                     //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // resize buffer
      // -----------------------------------// increase the new size to
      bufferSize=((int)(msgLng/1024)+1)*1024;   // next full kB, 
      buffer=resizeMqMessageBuffer(buffer     , // f.e. 4500Byte to 5k
                                  &bufferSize );// reallocating the buffer
      if( !buffer )                         //
      {                                     //
        sysRc = errno ;                     //
        if( sysRc == 0 ) sysRc = 1 ;        //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // read the message with new buffer size
      // -------------------------------------------------
      reason = mqGet( _hCon      ,          // connection handle
                      _hGetQ     ,          // pointer to queue handle
                      buffer     ,          // message buffer
                      &msgLng    ,          // buffer length
                      &md        ,          // message descriptor
                      gmo        ,          // get message option
                      5000      );          // wait interval in milliseconds
                                            //
      switch( reason )                      //
      {                                     //
        case MQRC_NONE: break;              //
        default:                            //
        {                                   //
	  sysRc = reason;                   //
          goto _door;                       //
        }                                   //
      }                                     //
    }                                       //
  }                                         //
#endif
  _door:

  logFuncExit( );
  return sysRc ;
}

/******************************************************************************/
/*  read message            */
/******************************************************************************/
int readMessage( MQHCONN _hCon    ,  // connection handle   
                 MQHOBJ  _hGetQ   ,  // get queue handle
		 PMQMD   _pMd     ,  // message descriptor (set to default) 
                 PMQVOID _pBuffer ,
                 PMQLONG _pMsgLng )
{
  logFuncCall() ;               

  int sysRc = 0 ;
  MQLONG reason ;

  MQGMO gmo = {MQGMO_DEFAULT};    // get message option set to default

  MQLONG newMsgLng ;

  // -----------------------------------------------------
  // read the message
  // -----------------------------------------------------
  memcpy( _pMd->MsgId       ,               // flash message id, 
          MQMI_NONE         ,               // 
          sizeof(MQBYTE24) );               // MQBYTE24  
  _pMd->Version = MQMD_VERSION_2;           //
                                            //
  gmo.MatchOptions = MQMO_MATCH_MSG_ID;     // 
  gmo.Options      = MQGMO_CONVERT    +     //
                     MQGMO_SYNCPOINT  ,     //
                     MQGMO_WAIT       ;     //
  gmo.Version      = MQGMO_VERSION_3  ;     //
                                            //
  reason = mqGet( _hCon    ,                // connection handle
                  _hGetQ   ,                // pointer to queue handle
                  _pBuffer ,                // message buffer
                  _pMsgLng ,                // buffer length
                  _pMd     ,                // message descriptor
                  gmo      ,                // get message option
                  60000   );                // wait interval in milliseconds
                                            // (makes 1Minute)
  switch( reason )                          //
  {                                         //
    // ---------------------------------------------------
    // read was OK
    // ---------------------------------------------------
    case MQRC_NONE :                        //
    {                                       //
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // no message found -> OK
    // ---------------------------------------------------
    case MQRC_NO_MSG_AVAILABLE :            // no message available, necessary 
    {                                       //  for catching signals
      break;                                //
    }                                       //
                                            //
    // ---------------------------------------------------
    // an error occurred preventing later commit
    // ---------------------------------------------------
    case MQRC_BACKED_OUT:                   //
    {                                       //
      sysRc = reason;                       //
      goto _backout;                        //
    }                                       //
                                            //
    // ---------------------------------------------------
    // message buffer to small for the (physical) message 
    //   - rollback
    //   - resize buffer
    //   - re-read message
    // ---------------------------------------------------
    case MQRC_TRUNCATED_MSG_FAILED :        // 
    {                                       //
      logMQCall( WAR, "MQGET", reason );    //
                                            //
      sysRc = reason;                       //
      if( reason != MQRC_NONE )             //
      {                                     //
        sysRc = reason;                     //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // resize buffer
      // -----------------------------------// increase the new size to
      newMsgLng=((int)((*_pMsgLng)/1024)+1)*1024;// next full kB, 
      _pBuffer=resizeMqMessageBuffer(_pBuffer ,// f.e. 4500Byte to 5k
                                  &newMsgLng );// reallocating the buffer
      if( !_pBuffer )                         //
      {                                     //
        sysRc = errno ;                     //
        if( sysRc == 0 ) sysRc = 1 ;        //
        goto _door;                         //
      }                                     //
                                            //
      // -------------------------------------------------
      // read the message with new buffer size
      // -------------------------------------------------
      reason=mqGet( _hCon    ,              // connection handle
                    _hGetQ   ,              // pointer to queue handle
                    _pBuffer ,              // message buffer
                    _pMsgLng ,              // buffer length
                    _pMd     ,              // message descriptor
                    gmo      ,              // get message option
                    60000   );              // wait interval in milliseconds
                                            //
      switch( reason )                      //
      {                                     //
        case MQRC_NONE: break;              //
        default:                            //
        {                                   //
	  sysRc = reason;                   //
          goto _door;                       //
        }                                   //
      }                                     //
    }                                       //
                                            //
    // ---------------------------------------------------
    // any other error
    // ---------------------------------------------------
    default:                                //
    {                                       //
      logMQCall(ERR,"MQGET",reason);      //
      sysRc = reason;                //
      goto _backout ;                  //
    }                                  //
  }                                         //
                                          //
  // -------------------------------------------------------
  // exit point for OK and Error
  // -------------------------------------------------------
  _door:

  logFuncExit( );
  return sysRc ;

  // -------------------------------------------------------
  // exit point for Error with rollback
  // -------------------------------------------------------
  _backout:

  reason = mqRollback( _hCon );         // roll back 
  if( reason != MQRC_NONE )             //
  {                                     //
    sysRc = reason;                     //
  }                                     //
 
  logFuncExit( );
  return sysRc ;

}
